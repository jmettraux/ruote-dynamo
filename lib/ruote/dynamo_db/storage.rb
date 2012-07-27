require 'ruote/storage/base'
require 'ruote/dynamo_db/version'

module Ruote
  module DynamoBD
    def self.create_table(connection, re_create=false, table_name='documents')
      
    end

    class Storage
      include Ruote::StorageBase

      def initialize(connection, table_name_prefix, options={})
        @connection = connection
        @table = connection.tables["#{table_prefix}.documents"].load_schema
      end
      # returns:
      # * true if the document has been deleted from the store
      # * a document when the rev has changed
      # * nil when successfully stored
      def put(doc, opts = {})
        if doc['rev']
          document = get(doc['type'], doc['_id'])
        end
        return true unless document
        return document if document['_rev'] != doc['_rev']
        
        new_revision = doc['_rev'].to_i + 1
        #TODO add error handling if create fails
        @table.items.create('ide' => doc['_id'],
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
            i.delete(:if => {:_rev < new_revision})
          end
        nil #success is nil, WTF?
      end
      
      # get a document by document type and key (_id)
      def get(type,key)
        document = @table.items.query(:hash_value => key,
          :typ => type)
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
        keys = key ? Array(key) : nil

        if opts[:limit] && !opts[:limit].nil?
          if keys && keys.first.is_a?(String)
            items = @table.items.where(:wfid).in(Array(key)).and("type").equals(type)
          else
            items = @table.items.where("type").equals(type).limit(opts[:limit])
          end
        else
          if keys && keys.first.is_a?(String)
            items = @table.items.where(:wfid).in(Array(key)).and("type").equals(type)
          else
            items = @table.items.where("type").equals(type)
          end
        end
        
        if !items.nil? && !items.empty?
          if opts[:count]
            # TODO - should this be length, or count - when count could be another request
            return items.count
          else
            return 0
          end
        end

        #not supporting sorting yet...
        # now comes client side order...ugh

        docs = items.collect{|d| Rufus::Json.decode(d[:doc])}
        
        #return only those documents filter by a regexp
        if keys && keys.first.is_a?(Regexp)
          docs.select {|d| keys.find {|k| k.match(doc["_id"])}}
        else
          docs
        end

      end
      
      # Return a list of ids for the given document type
      #
      def ids(type)
      end

      # Removes all msgs, schedules, errors, expressions and workitems.
      #
      # It's used mostly when testing workflows, usually when cleaning the
      # engine/storage before a workflow run.
      #
      def clear
      end
      
      # Clean the store
      #
      def purge!
      end
      
      # Add a new document type to the store. Some storages might need it.
      #
      def add_type(type)
      end
      
      # Clean the store for the given document type
      #
      def purge_type!(type)
      end

      protected
      def extract_wfid(doc)
        doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
      end


    end
  end
end
