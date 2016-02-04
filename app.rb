require 'redis'
require 'json'
class App
  def initialize
    @redis_client = Redis.connect(url: ENV['PGL_REDIS_SERVER'])
  end

  def run
    while true do
      p ENV['PGL_REDIS_SERVER']
      puts "waiting..."

      channel, request = @redis_client.brpop('sync_files')

      request_json = JSON.parse(request)

      puts "processing #{request_json["id"]}"
      process(request_json)
    end
  end

  def process(request)
    puts "processing #{request["table"]} company: #{request["company"]}"
    # Params
    table = request["table"] #activities
    company = request["company"] # canada
    url = request["url"] # http://amazon....
    fields = request["fields"] # id,product_type,description

    # Env Vars
    db_connect = "#{ENV['PGL_POSTGRES_SERVER']}?sslmode=require&tablename=#{company}.#{table}"
    filename = "#{company}.#{table}.#{Time.now.strftime("%Y%m%d%M%S")}.load"

    file_csv = `curl -o ./tmp/#{filename}.csv "#{url}"`

    # Creating load file
    contents = "LOAD CSV\n"+
      "FROM './tmp/#{filename}.csv'\n"+
      "HAVING FIELDS\n"+
      "(\n"+
      "#{fields}\n"+
      ")\n"+
      "INTO #{db_connect}\n"+
      "TARGET COLUMNS\n"+
      "(\n"+
      "#{fields}\n"+
      ")\n"+
      "WITH fields terminated by ';',\n"+
      "skip header = 1\n"+
      ";"

    # Writing load file
    out_file = File.new(ENV['PGL_PATH']+filename, "w")
    out_file.puts(contents)
    out_file.close

    # Calling pgloader file.load
    ret = `pgloader #{ENV['PGL_PATH']+filename}`

    remove_file = `rm tmp/#{filename}.csv`

    # Managing response
    if $?.success?
      # Write success log file
      out_file = File.new(ENV['PGL_PATH'] + 'logs/' + filename + ".log", "w")
      out_file.puts(ret)
      out_file.close

      response = if ret.include? "Total import time"
                   ret = ret.split("Total import time")
			p ret
                   ret = ret[1].gsub(/\s+/, ' ').split(" ")
p ret
puts ret
                   JSON.generate(:read => ret[0], :imported => ret[1], :errors => ret[2])
                 end

      @redis_client.set(request['id'], response)

    else
      # Write error log file
      out_file = File.new(ENV['PGL_PATH'] + filename + ".error", "w")
      out_file.puts(ret)
      out_file.close

      @redis_client.rpush(request['id'], false)
    end

  end
end
