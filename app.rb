require 'redis'
require 'json'
require 'open-uri'
require './load_generator.rb'
require './amazon_client'

class App
  def initialize
    @redis_client = Redis.connect(url: ENV['PGL_REDIS_SERVER'])
  end

  def run
    while true do
      puts "waiting..."
puts ENV['PGL_REDIS_SERVER']

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

    ret = `pgloader --root-dir "/opt/pgloader-ms/" #{ENV['PGL_PATH'] + "/" + @filename}`

    if $?.success?
      log_filename = generate_log_file(ret)
      response = get_statistics(ret).merge({log: log_filename})
      @redis_client.set(request['id'], response.to_json)
    else
      log_filename = generate_log_file(ret, error: true)
      @redis_client.set(request['id'], { "status" => "error", log: log_filename }.to_json)
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
    amazon_client = AmazonClient.new(ENV['PGL_IMPORT_BUCKET'])
    amazon_client.secure_url(@request['file'])
  end

  def clear_files
    File.delete("tmp/#{@filename}.csv")
    File.delete(@filename)
  end

  def generate_log_file(content, error: false)
    amazon_client = AmazonClient.new(ENV['PGL_LOGS_BUCKET'])
    subfolder = error ? 'error' : 'success'
    file_name = "#{Time.now.strftime("%Y%m%d")}/#{subfolder}/#{@request["id"]}_#{@filename}.log"
    amazon_client.put_file(file_name, content)

    file_name
  end

  def get_statistics(ret)
    if ret.include? "Total import time"
      ret = ret.split("Total import time")
      ret = ret[1].gsub(/\s+/, ' ').split(" ")
      { :read => ret[0], :imported => ret[1], :errors => ret[2] }
    end
  end

end
