class LoadGenerator
  def self.generate(filename, request)
    table = request["table"]
    company = request["company"]
    url = request["url"]
    fields = request["fields"]
    db_connect = "#{ENV['PGL_POSTGRES_SERVER']}?sslmode=require&tablename=#{company}.#{table}"

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

    File.write(ENV['PGL_PATH'] + filename, contents)
  end
end
