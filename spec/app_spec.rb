require 'timecop'
require './app.rb'

RSpec.describe App do

  context "#process" do
    let(:request){
      {
        "id" => "80",
        "table" => "activities",
        "company" => "company_slug",
        "url" => "request_url",
        "synchro_id" => "123",
        "fields" => "name,surname,age"
      }
    }

    let(:redis) { double("redis") }
    let(:s3){ instance_double(AmazonClient) }

    let(:csv_file){
        "name\tsurname\tcity\rJohn\tDoe\tDallas\rJane\tDoe\tPhoenix\r"
    }

    before do
      allow(LoadGenerator).to receive(:generate).and_return(true)
      allow(IO).to receive(:copy_stream).and_return(true)
      allow(File).to receive(:delete).and_return(true)
      allow(File).to receive(:write).and_return(true)

      allow(redis).to receive(:set)
      allow(redis).to receive(:rpush)
      allow(Redis).to receive(:connect).and_return(redis)

      allow(s3).to receive(:secure_url).and_return("secure_url")
      allow(s3).to receive(:put_file).and_return("log_filename")
      allow(AmazonClient).to receive(:new).and_return(s3)

      allow(File).to receive(:open).with("./tmp/company_slug.activities.201602113344.load.csv", anything) { StringIO.new(csv_file) }
      allow(File).to receive(:open).with("./tmp/company_slug.activities.201602113344.load.csv", anything, anything) { StringIO.new(csv_file) }

      ENV['PGL_PATH'] = 'pgloader_path'
    end

    let(:app){ app = App.new }

    before do
      allow(app).to receive(:open).and_return("content_url")
      allow(app).to receive(:`).and_return("Total import time
                                                       1234 456 678")
      allow($CHILD_STATUS).to receive(:success?).and_return(true)
    end

    it "generates the load file" do

      Timecop.freeze('2016-02-11 22:33:44') do
        app.process(request)

        expect(LoadGenerator).to have_received(:generate).with(
          /company_slug.activities(.*).load/,
          request
        )
      end
    end

    it "downloads the csv file" do
      Timecop.freeze('2016-02-11 22:33:44') do
        app.process(request)

        expect(app).to have_received(:open).with("secure_url")
        expect(IO).to have_received(:copy_stream).with(
          "content_url",
          /company_slug.activities(.*).load.csv/,
        )
      end
    end

    it "calls to the system" do
      Timecop.freeze('2016-02-11 22:33:44') do
        app.process(request)

        expect(app).to have_received(:`).with(
          /pgloader pgloader_path\/company_slug.activities(.*).load/
        )
      end
    end

    it "sends to redis the statistics" do
      Timecop.freeze('2016-02-11 22:33:44') do
        app.process(request)

        expect(redis).to have_received(:set).with("80", {
          "read" =>  "1234",
          "imported" => "456",
          "errors" => "678",
          "log" => "#{Time.now.strftime("%Y%m%d")}/success/80_company_slug.activities.#{Time.now.strftime("%Y%m%d%M%S")}.load.log"
        }.to_json)
      end
    end

    it "clears the filesystem" do
      Timecop.freeze('2016-02-11 22:33:44') do
        app.process(request)
        expect(File).to have_received(:delete).with(/^company_slug.activities(.*).load/)
        expect(File).to have_received(:delete).with(/tmp\/company_slug.activities(.*).load.csv/)
      end
    end

    it "writes logs" do
      expect_output = "Total import time
      1234 123 123"
      allow(app).to receive(:`).and_return(expect_output)

      Timecop.freeze('2016-02-11 22:33:44') do
        app.process(request)

        expect(s3).to have_received(:put_file)
          .with(/20160211\/success\/80_company_slug.activities.(.*).load.log/, expect_output)
      end
    end

    context "when the execution pgloeader went wrong" do
      before do
        allow($CHILD_STATUS).to receive(:success?).and_return(false)
        app.process(request)
      end

      it "sets the statatistic of redis to false" do

        Timecop.freeze('2016-02-11 22:33:44') do
          logfile = "#{Time.now.strftime("%Y%m%d")}/error/80_company_slug.activities.#{Time.now.strftime("%Y%m%d%M%S")}.load.log"
          expect(redis).to have_received(:set).with("80", { "status" => "error", "log" => logfile }.to_json)
        end
      end

      it "writes logs" do
        allow(app).to receive(:`).and_return("hello there")
        Timecop.freeze('2016-02-11') do
          app.process(request)

          expect(s3).to have_received(:put_file)
            .with(/20160211\/error\/80_company_slug.activities.(.*).load.log/, "hello there")
        end
      end
    end

  end

end
