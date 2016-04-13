class LoadGenerator
  def self.generate(filename, request)
    table = request["table"]
    company = request["company"]
    fields = request["fields"]

    postgresql = if request["env"] == 'staging'
                   ENV['PGL_POSTGRES_SERVER_STAGING']
                 else
                   ENV['PGL_POSTGRES_SERVER']
                 end

    db_connect = "#{postgresql}?sslmode=require&tablename=#{company}.#{table}"

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
