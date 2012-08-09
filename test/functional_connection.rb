#
# testing ruote-dynamo-db
#
#

require 'rufus-json'
Rufus::Json.backend = :json_pure

require 'aws-sdk'
require 'logger'
require 'ruote-dynamo-db'
require 'ruby-debug'

unless File.exists?(File.expand_path(File.join(File.dirname(__FILE__), "dynamo.yml")))
  raise "Please create a dynamo.yml file in #{File.dirname(__FILE__)} based on dynamo.yml.example\n"
end

unless $dynamo_db

  settings = YAML::load_file(File.expand_path(File.join(File.dirname(__FILE__), "dynamo.yml")))

  aws_settings = {
    :access_key_id => settings['access_key_id'],
    :secret_access_key => settings['secret_access_key']
  }

  logger = nil

  if ARGV.include?('-l') || ARGV.include?('--l')
    FileUtils.rm('debug.log') rescue nil
    file = if File.exists?('debug.log')
             File.open('debug.log', File::WRONLY | File::APPEND)
           else
             File.open('debug.log', File::WRONLY | File::APPEND | File::CREAT)
           end

    logger = Logger.new(file)
  elsif ARGV.include?('-ls') || ARGV.include?('--ls')
    logger = Logger.new($stdout)
  end

  aws_settings[:logger] = logger if logger
  AWS.config(aws_settings)

  $dynamo_db = AWS::DynamoDB.new(aws_settings)
  $table_prefix = settings["table_prefix"]

  AWS.config(aws_settings)

  Ruote::DynamoDB.create_table($dynamo_db,
                               $table_prefix,
                               true,
                               {:read_capacity_units => 20, :write_capacity_units => 20 })
end


def new_storage(opts)
  Ruote::DynamoDB::Storage.new($dynamo_db, $table_prefix)
end

