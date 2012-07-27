require 'ruote/storage/base'
require 'ruote/dynamo_db/version'

module Ruote
  module DynamoBD
    def self.create_table(connection, re_create=false, table_name='documents')
      
    end

    class Storage
      include Ruote::StorageBase

      attribute :dynamo_db

      def initialize(connection, options={})
      end
      # returns:
      # * true if the document has been deleted from the store
      # * a document when the rev has changed
      # * nil when successfully stored
      def put(doc, opts = {})
      end
      
      # get a document by document type and key (_id)
      def get(type,key)
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
    end
  end
end
