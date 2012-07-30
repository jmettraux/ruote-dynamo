require 'ruote/storage/base'
require 'ruote/dynamo_db/version'

module Ruote
  module DynamoDB

    SCHEMA = {:hash_key => {:ide => :string}, :range_key => {:typ => :string}}

    def self.create_table(connection, table_prefix, recreate = false)
      table_name = "#{table_prefix}.documents"
      if recreate
        table = connection.tables[table_name]
        # connection always returns a table, even if it doesn't exist
        begin 
        if table.exists?
          table_exists = true
          table.delete 
          while table.status == :deleting
            sleep(1)
          end
        end
        rescue AWS::DynamoDB::Errors::ResourceNotFoundException => e
          if table_exists
            $stdout << "Table #{table_name} has been deleted"
          else
            $stdout << "Table #{table_name} does not exist"
          end
        end
      end
      table = connection.tables.create(table_name, 10, 5, SCHEMA)
      while table.status == :creating
        sleep(1)
      end
    end

    class Storage
      include Ruote::StorageBase

      def initialize(connection, table_prefix, options={})
        @connection = connection
        @table = connection.tables["#{table_prefix}.documents"].load_schema
      end
      # returns:
      # * true if the document has been deleted from the store
      # * a document when the rev has changed
      # * nil when successfully stored
      def put(doc, opts = {})
        if doc['_rev']
          document = get(doc['type'], doc['_id'])
          return true unless document
          return document if document['_rev'] != doc['_rev']
        end
        
        new_revision = doc['_rev'].to_i + 1
        #TODO add error handling if create fails
        values = {'ide' => doc['_id'],
          'rev' => new_revision,
          'typ' => doc['type'],
          'doc' => Rufus::Json.encode(doc),
          'wfid' => extract_wfid(doc),
          'participant_name' => doc['participant_name'])

        # delete all items it the database whose doc 'typ'
        # is the same as doc, whose 'ide' is the same as 'doc['_id'],
        # and whose revision is less that the old revision
        items = @table.items.query(:hash_value => doc[_id],
                                   :range_value => doc["type"],
                                   :select => doc['_rev'])
        
        unless items.nil? || items.empty?
          items.each do |i|
            if i[:rev] < new_revision
              i.delete
            end
          end
        end
        nil #success is nil, WTF?
      end
      
      # get a document by document type and key (_id)
      def get(type,key)
        document = @table.items.query(:hash_value => key,
          :range_value => type).first
        document ? Rufus::Json.decode(document[:doc]) : nil
      end

      # Delete a document
      #
      # returns:
      # * true if already deleted
      # * a document if the rev of the given document is not the rev of the current
      #   document
      # * nil when successfully removed
      #
      def delete(doc)
        unless doc['_rev']
          raise ArgumentError.new('no _rev for doc')
        end

        items = @table.items.query(:hash_value => doc['_id'],
          :typ => doc['type']).where(:rev).equals(doc['_rev'])
        count = 0;
        unless items.nil? || items.empty?
          items.each do |i|
            # TODO handle delete errors
            i.delete
            count += 1
          end
        end
        
        if count < 1
          return get(doc['type'], doc['_id']) || true
        end
        nil #Who returns nil of success?
      end


      # Get many documents of a certain type
      #
      # input:
      # - When key is not specified, return everything from the store
      # - Else, do Array(key) and match the collected document id to the given list
      #
      # opts:
      # [:descending]    Return a list, sorted by _id in descending order
      # [:count]         Just return a count
      # [:skip]          Skip X results
      # [:limit]         Limit the list to a length of X
      #
      # returns:
      # * An Integer (when :count was specified)
      # * An array of documents
      #
      def get_many(type, key=nil, opts={})
        # TODO, refactor
        keys = key ? Array(key) :nil
        if !opts[:count].nil? && !opts[:count].empty? && opts[:count].is_a?(Integer)
          if keys && keys.first.is_a?(String)
            @table.items.where(:typ).equals(type).and(:wfid).in(keys).count
          else
            @table.items.where(:typ).equals(type).count
          end
        end

        #TODO, support skip
        raise "Does not support :skip options" unless opts[:skip].nil?

        if !opts[:limit].nil? && opts[:limit].is_a?(Integer)
          docs = if keys && keys.first.is_a?(String)
                   @table.items.where(:typ).equals(type).and(:wfid).in(keys).limit(opts[:limit])
                 else
                   @table.items.where(:typ).equals(type).limit(opts[:limit])
                 end
          sort_items_by_ide_and_rev!(items, opts[:descending])
        end

        #sort again, but only by :ide
        docs = docs.each_with_object({}) { |doc, h|
          h[doc[:ide]] = doc
        }.values.sort_by { |h|
          h[:ide]
        }
        docs = opts[:descending] == true ? docs.reverse : docs

        #expand the json
        docs = docs.collect{ |d| Rufus::Json.decode(d[:doc]) }
        
        # select the only those docs, that match the regex by _id
        if keys && keys.first.is_a?(Regexp)
          docs.select { |doc| keys.find { |key| key.match(doc['_id']) } }
        else
          docs
        end
      end
      
      # Return a list of ids for the given document type
      #
      def ids(type)
        @table.items.where(:typ).equals(type).select(:ide).uniq.sort
      end

      # Removes all msgs, schedules, errors, expressions and workitems.
      #
      # It's used mostly when testing workflows, usually when cleaning the
      # engine/storage before a workflow run.
      #
      def clear
        name = @table.name
        @table.delete
        @connection.tables.create(name, 10, 5, SCHEMA)
      end
      
      # Clean the store
      #
      def purge!
        clear
      end
      
      # Add a new document type to the store. Some storages might need it.
      #
      def add_type(type)
        # like sequel storage, we are donig nothing
      end
      
      # Clean the store for the given document type
      #
      def purge_type!(type)
        @table.where(:typ).equals(type).delete
      end

      protected
      def extract_wfid(doc)
        doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
      end


      def sort_items_by_ide_and_rev!(items, order)
        # TODO - refactor
        items.sort do |x,y|
          if order[:descending]
            if x[:ide] < y[:ide] && x[:rev] < y[:rev]
              -1
            elsif x[:ide] > y[:ide] && x[:rev] > y[:rev]
              1
            else 0
            end
          else
            if x[:ide] > y[:ide] && x[:rev] > y[:rev]
              -1
            elsif x[:ide] < y[:ide] && x[:rev] < y[:rev]
              1
            else 0
            end
          end
        end
      end
    end
  end
end


