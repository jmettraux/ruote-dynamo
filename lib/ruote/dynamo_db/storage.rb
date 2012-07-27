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
    end
  end
end
