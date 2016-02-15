require 'aws-sdk'
class AmazonClient
  def initialize(bucket)
    client = Aws::S3::Client.new
    resource = Aws::S3::Resource.new(client: client)
    @bucket = resource.bucket(bucket)
  end

  def secure_url(filename, expiration = 120)
    puts filename
    @bucket.object(filename).presigned_url(:get, expires_in: expiration)
  end

  def put_file(filename, content)
    @bucket.object(filename).put(body: content)
  end
end
