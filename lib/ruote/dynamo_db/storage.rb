# Copyright (C) 2012 Medidata Solutions Inc.
#  
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#  
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#  
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'ruote/storage/base'
require 'ruote/dynamo_db/version'

module Ruote
  module DynamoDB

    SCHEMA = {:hash_key => {:ide => :string}, :range_key => {:typ => :string}}

    def self.create_table(connection, table_prefix, recreate = false, options = {})
      table_name = "#{table_prefix}.documents"
      if recreate
        table = connection.tables[table_name]
        # connection always returns a table, even if it doesn't exist
        begin 
        if table.exists?
          table_exists = true
          table.delete
          # Dynamo is slow 
          while table.status == :deleting
            sleep(3)
          end
        end
        rescue AWS::DynamoDB::Errors::ResourceNotFoundException => e
          if table_exists
            $stdout << "Table #{table_name} has been deleted\n"
          else
            $stdout << "Table #{table_name} does not exist\n"
          end
        end
      end
      read_capacity = options[:read_capacity_units] || 10
      write_capacity = options[:write_capacity_units] || 5
      table = connection.tables.create(table_name, read_capacity, write_capacity, SCHEMA)
      # Dynamo is slow
      while table.status == :creating
        sleep(3)
      end
    end

    class Storage
      include Ruote::StorageBase

      def initialize(connection, table_prefix, options={})
        @connection = connection
        @table = connection.tables["#{table_prefix}.documents"].load_schema
      end

      def put_msg(action,options)
        # like the sequel storage, overriding, and hence simplifying
        # inserting a message
        create_item(prepare_msg_doc(action,options),1)
        nil
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

        create_item(doc, new_revision, opts[:update_rev])

        # delete all items it the database whose doc 'typ'
        # is the same as doc, whose 'ide' is the same as 'doc['_id'],
        # and whose revision is less that the old revision
        items = @table.items.query(:hash_value => doc["_id"],
                                   :range_value => doc["type"])
        items.each do |i|
          if i.attributes[:rev].to_i < new_revision
            i.delete
          end
        end

        nil #success is nil, WTF?
      end
      
      # get a document by document type and key (_id)
      def get(type,key)
        document = @table.items.query(:hash_value => key,
          :range_value => type).first
        #TODO - sort, and return in rev
        document ? Rufus::Json.decode(document.attributes[:doc]) : nil
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
                                   :range_value => doc['type'])
        count = 0;
        items.each do |i|
          # TODO handle delete errors
          if i.attributes[:rev].to_i == doc['_rev']
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
        if opts[:count]
          if keys && keys.first.is_a?(String)
            return @table.items.where(:typ => type).and(:wfid).in(*keys).count
          else
            return @table.items.where(:typ => type).count
          end
        end

        doc_selector = if keys && keys.first.is_a?(String)
                         @table.items.where(:typ => type).and(:wfid).in(*keys)
                       else
                         @table.items.where(:typ => type)
                       end

        # Dynamo DB has no ability to skip a number of items,
        # so we manually implement skip
        docs = []
        # load them all in memory, to reduce db hits
        if opts[:limit]
          limit = if opts[:skip] then opts[:limit] + opts[:skip]
            else opts[:limit]
            end
          doc_selector.select(:limit => limit) do |item_data|
            break if opts[:skip] && docs.length < opts[:skip]
            docs << item_data.attributes
          end
        else
          doc_selector.select do |item_data|
            break if opts[:skip] && docs.length < opts[:skip]
            docs << item_data.attributes
          end
        end

        sort_docs_by_ide_and_rev!(docs, opts[:descending])

        docs = docs.sort_by{|doc| doc["ide"]}
        docs = opts[:descending] == true ? docs.reverse : docs

        #expand the json
        docs = docs.collect do |doc|
          Rufus::Json.decode(doc['doc'])
        end
        
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
        ids = []
        @table.items.where(:typ => type).select(:ide) do |doc|
          ids << doc.attributes["ide"]
        end
        ids.uniq.sort
      end

      # Clean the store
      #
      def purge!
        @table.items.each {|i| i.delete}
      end
      
      # Add a new document type to the store. Some storages might need it.
      #
      def add_type(type)
        # like sequel storage, we are donig nothing
      end
      
      # Clean the store for the given document type
      #
      def purge_type!(type)
        @table.items.where(:typ => type).each{|i|i.delete}
      end

      def shutdown
        # seems to be require for testing, but there is no
        # such thing as a dynamo db shutdown
      end

      protected
      def extract_wfid(doc)
        doc['wfid'] || (doc['fei'] ? doc['fei']['wfid'] : nil)
      end


        # TODO - refactor
      def sort_docs_by_ide_and_rev!(docs, order)
        docs.sort do |x,y|
          x_ide = x['ide']
          x_rev = x['rev'].to_i
          y_ide = y['ide']
          y_rev = y['rev'].to_i
          if order
            if x_ide < y_ide && x_rev < y_rev
              -1
            elsif x_ide > y_ide && x_rev > y_rev
              1
            else 0
            end
          else
            if x_ide > y_ide && x_rev > y_rev
              -1
            elsif x_ide < y_ide && x_rev < y_rev
              1
            else 0
            end
          end
        end
      end

      def create_item(doc,revision,update_rev = false)
        #TODO add error handling if create fails
        doc = doc.send(
          update_rev ? :merge! : :merge,
          {'_rev' => revision, 'put_at' => Ruote.now_to_utc_s})

        values = {'ide' => doc['_id'],
          'rev' => doc["_rev"],
          'typ' => doc['type'],
          'doc' => Rufus::Json.encode(doc)}

        wfid = extract_wfid(doc)
        
        # these conditionals are here, because
        # DynamoDB doesn't support nil or empty strings
        if wfid
          values['wfid'] = wfid
        end

        unless doc['participant_name'].nil?
          values['participant_name'] = doc['participant_name']
        end

        @table.items.create(values)
      end
    end
  end
end


