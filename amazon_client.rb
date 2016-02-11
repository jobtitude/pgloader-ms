require 'aws-sdk'
class AmazonClient
  def initialize(bucket)
    @client = Aws::S3::Client.new
    @bucket = @client.buckets[bucket]
  end

  def secure_url(filename, expiration = 120)
    @bucket.objects[filename].url_for(:read, :expires => expiration).to_s
  end

  def put_file(filename, content)
    @bucket.object(filename).put(body: content)
  end
end
