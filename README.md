# route-dynamo

Ruote Dynamo is a [Amazon DynamoDB](http://aws.amazon.com/dynamodb/) storage implementation of [Ruote](http://ruote.rubyforge.org/).

# Status
The code is under heavily development, and not production ready. All development
will be published on the master branch until the code reaches stability, where it
will be tagged, and development started on topic branches.

The code is a rough port of the [route-sequel](https://github.com/jmettraux/ruote-sequel)
storage implementation

Despite the presence of a gemspec, the code has not been published as a gem
on rubygems.org.

# Usage
Create the `documents` table on which the connection depends:
Creating a DynamoDB tables works as follows.
``` ruby
require 'aws-sdk'
require 'route'
require 'ruoute/dynamo_db/storage'

AWS.config(:access_key_id => "an access key from amazon",
           :secret_access_key => "a secret access key from amazon")

connection = AWS::DynamoDB.new(:access_key_id => "an access key from amazon",
		              :secret_access_key => "a secret access key from amazon")

recreate_db = true # will delete the current documents table

# 10 and 5 are actually the default values respectively, for the 
# table read and write capacities.
Ruote::DynamoDB.create_table(connection,
                               "prefix_to_documents_table",
                               recreate_db,
                               {:read_capacity_units => 10,
                                :write_capacity_units => 5})

```


Ccreate a connection to a `documents` table with the given prefix below.

``` ruby
require 'aws-sdk'
require 'route'
require 'ruoute/dynamo_db/storage'

AWS.config(:access_key_id => "an access key from amazon",
           :secret_access_key => "a secret access key from amazon")

connection = AWS::DynamoDB.new(:access_key_id => "an access key from amazon",
		              :secret_access_key => "a secret access key from amazon")

storage = Ruote::DynamoDB::Storage.new(connection,
                                       "prefix_to_documents_table")

dashboard = Route::Dashboard.new(Route::Worker.new(storage))
```


# TODO
* Optimize the lookups as much as possible, moving from scan operations to query
operations. 
* Refactor the code removing redundancies
* Unit tests

# License
Copyright (C) 2012 Medidata Solutions Inc.
 
Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:
 
The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.
 
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
