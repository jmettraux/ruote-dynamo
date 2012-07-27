# encoding: UTF-8

Gem::Specification.new do |s|

  s.name = 'ruote-dynamo-db'

  s.version = File.read(
    File.expand_path('../lib/ruote/dynamo_db/version.rb', __FILE__)
  ).match(/ VERSION *= *['"]([^'"]+)/)[1]

  s.platform = Gem::Platform::RUBY
  s.authors = [ 'Chad Albers' ]
  s.email = [ 'calbers@mdsol.com' ]
  s.homepage = 'http://mdsol.com'
  s.summary = 'AWS Dynamo DB storage for ruote (a workflow engine)'
  s.description = %q{
AWS Dynamo DB for ruote (a workflow engine)
  }

  #s.files = `git ls-files`.split("\n")
  s.files = Dir[
    'Rakefile',
    'lib/**/*.rb',
    '*.gemspec', '*.txt', '*.rdoc', '*.md'
  ]

  s.add_runtime_dependency 'ruote'
  s.add_development_dependency 'rake'
  s.require_path = 'lib'
end

