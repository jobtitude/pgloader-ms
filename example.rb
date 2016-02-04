require './app.rb'
require 'json'
redis_client = Redis.connect(url: ENV['PGL_REDIS_SERVER'])
request = {
  id: '80-123123',
  company: 'jobtitude',
  table: 'tablerone',
  fields: 'field1,field2',
  url: 'http://spatialkeydocs.s3.amazonaws.com/FL_insurance_sample.csv.zip'
}
redis_client.lpush('sync_files', JSON.generate(request))
