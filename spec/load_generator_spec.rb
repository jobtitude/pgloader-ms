require './load_generator.rb'

RSpec.describe LoadGenerator, "#generate" do
  context "when it gets the request" do

    let(:request) do
      {
        "table" => "activities",
        "company" => "company_slug",
        "url" => "url",
        "fields" => "name,surname,age"
      }
    end

    let(:table) { request["table"] }
    let(:company) { request["company"] }
    let(:url) { request["url"] }
    let(:fields) { request["fields"] }

    before do
      allow(File).to receive(:write).and_return(true)
      ENV['PGL_POSTGRES_SERVER'] = 'PG_SERVER'
      ENV['PGL_PATH'] = 'PG_PATH/'
    end

    it "generates a load file" do
      LoadGenerator.generate("filename", request)

      expect_content = "LOAD CSV\n"+
        "FROM './tmp/filename.csv'\n"+
        "HAVING FIELDS\n"+
        "(\n"+
        "#{fields}\n"+
        ")\n"+
        "INTO PG_SERVER?sslmode=require&tablename=#{company}.#{table}\n"+
        "TARGET COLUMNS\n"+
        "(\n"+
        "#{fields}\n"+
        ")\n"+
        "WITH fields terminated by ';',\n"+
        "skip header = 1\n"+
        ";"
      expect(File).to have_received(:write).with("PG_PATH/filename.load", expect_content)
    end
  end
end
