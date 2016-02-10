require 'redis'
require 'json'
require 'open-uri'
require './load_generator.rb'
require 'aws-sdk'

class App
  def initialize
    @redis_client = Redis.connect(url: ENV['PGL_REDIS_SERVER'])
  end

  def run
    while true do
      puts "waiting..."

      channel, request = @redis_client.brpop('sync_files')

      request_json = JSON.parse(request)

      puts "processing #{request_json["id"]}"
      process(request_json)
    end
  end

  def process(request)
    @request = request
    puts "processing #{request["table"]} company: #{request["company"]}"
    @filename = "#{request["company"]}.#{request["table"]}.#{Time.now.strftime("%Y%m%d%M%S")}.load"

    download_file
    generate_load_file

    ret = `pgloader #{ENV['PGL_PATH'] + "/" + @filename}`

    if $?.success?
      generate_log_file(ret)
      response = get_statistics(ret)
      @redis_client.set(request['id'], response)
    else
      generate_log_file(ret, error: true)
      @redis_client.set(request['id'], { "status" => "error" }.to_json)
    end

    clear_files
  end

  private

  def generate_load_file
    LoadGenerator.generate(@filename, @request)
  end

  def download_file
    IO.copy_stream(open(secure_url), "./tmp/#{@filename}.csv")
  end

  def secure_url
    client = Aws::S3::Client.new
    bucket = client.buckets['loyalguru-imports']
    bucket.objects[@request['file']].url_for(:read, :expires => 120).to_s
  end

  def clear_files
    File.delete("tmp/#{@filename}.csv")
    File.delete(@filename)
  end

  def generate_log_file(content, error: false)
    file_name = "#{ENV['PGL_PATH']}logs/#{@request["id"]}_#{@filename}"

    if error
      file_name += '.error'
    end

    File.write(file_name + ".log", content)
  end

  def get_statistics(ret)
    if ret.include? "Total import time"
      ret = ret.split("Total import time")
      ret = ret[1].gsub(/\s+/, ' ').split(" ")
      JSON.generate(:read => ret[0], :imported => ret[1], :errors => ret[2])
    end
  end

end
