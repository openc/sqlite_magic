require_relative '../spec_helper'
require 'sqlite_magic'

describe SqliteMagic do
  it "should define SqliteMagicError exception as subclass of StandardError" do
    SqliteMagic::Error.superclass.should be StandardError
  end

  it "should define DatabaseError exception as subclass of Error" do
    SqliteMagic::DatabaseError.superclass.should be SqliteMagic::Error
  end

  it "should define InvalidDataError exception as subclass of Error" do
    SqliteMagic::NoSuchTable.superclass.should be SqliteMagic::Error
  end

  describe SqliteMagic::Connection do
    before do
      @dummy_db = double('dummy_sqlite3_database')
      SQLite3::Database.stub(:new).and_return(@dummy_db)
      @connection = SqliteMagic::Connection.new
    end

    describe "initialisation" do
      it "should return a type of connection" do
        connection = SqliteMagic::Connection.new
        connection.should be_kind_of SqliteMagic::Connection
      end

      it "should open up a Sqlite3 database with default name" do
        SQLite3::Database.should_receive(:new).with('sqlite.db',{}).and_return(@dummy_db)
        connection = SqliteMagic::Connection.new
      end

      it "should use given db_name when setting up Sqlite3" do
        SQLite3::Database.should_receive(:new).with('path/to/mynew.db',{}).and_return(@dummy_db)
        connection = SqliteMagic::Connection.new('path/to/mynew.db')
      end

      it "should pass options when opening Sqlite3 db" do
        SQLite3::Database.should_receive(:new).with('path/to/mynew.db', :foo => 'bar').and_return(@dummy_db)
        connection = SqliteMagic::Connection.new('path/to/mynew.db', :foo => 'bar')
      end

      it "should store Sqlite3 database in @database instance variable" do
        connection = SqliteMagic::Connection.new
        connection.instance_variable_get(:@database).should == @dummy_db
      end

      it "should set busy_timeout if passed as options when opening Sqlite3 db" do
        SQLite3::Database.should_receive(:new).with('path/to/mynew.db', {}).and_return(@dummy_db)
        @dummy_db.should_receive(:busy_timeout=).with(12345)
        connection = SqliteMagic::Connection.new('path/to/mynew.db', :busy_timeout => 12345)
      end

    end

    it "should have #database accessor" do
      SqliteMagic::Connection.new.database.should == @dummy_db
    end

    describe "#execute" do
      before do
        response_data = [['heading 1', 'heading 2', 'heading 3'],
                         ['some val 1', 'some val 2', 'some val 3'],
                         ['another val 1', 'another val 2', 'another val 3']]
        @dummy_db.stub(:execute2).and_return(response_data)
      end

      it "should use #execute2 to run query on database" do
        @dummy_db.should_receive(:execute2).with('some query',[:foo])

        @connection.execute('some query',[:foo])
      end

      it "should return array of hashes" do
        result = @connection.execute('some query',[:foo])
        result.should be_kind_of Array
        result.first.should be_kind_of Hash
        result.size.should == 2
      end

      it "should use map results to hash with headings as keys" do
        result = @connection.execute('some query',[:foo])
        result.first['heading 1'].should == 'some val 1'
      end

      context 'and only headings returned' do
        it "should return empty array" do
          @dummy_db.stub(:execute2).and_return([['foo1','foo2']])
          @connection.execute('some query',[:foo]).should == []
        end
      end

      context 'and nil passed as second argument' do
        it "should not pass bind vars to #execute2" do
          @dummy_db.should_receive(:execute2).with('some query')

          @connection.execute('some query',nil)
        end
      end

      context 'and table does not exist' do
        before do
          @dummy_db.stub(:execute2).
                      and_raise(SQLite3::SQLException.new("no such table: foo_table") )
        end

        it 'should raise NoSuchTable exception' do
          lambda { @connection.execute('some query') }.should raise_error(SqliteMagic::NoSuchTable)
        end
      end

      context 'and other SQLite3 error raised' do
        before do
          @other_ex = SQLite3::SQLException.new("something else went wrong")
          @dummy_db.stub(:execute2).
                      and_raise(@other_ex)
        end

        it 'should raise exception' do
          lambda { @connection.execute('some query') }.should raise_error(@other_ex)
        end
      end
    end

    describe '#save_data' do
      before do
        @connection = SqliteMagic::Connection.new
        @data = [{:foo => 'bar', :foo2 => 'bar2', :foo3 => 'bar3'},
                 {:foo2 => 'baz2', :foo3 => 'baz3', :foo4 => 'baz4'}]
        @unique_keys = [:foo2,:foo3]
        @expected_query_1 = "INSERT OR REPLACE INTO foo_table (foo,foo2,foo3,foo4) VALUES (:foo,:foo2,:foo3,:foo4)"
        @expected_query_2 = "INSERT OR REPLACE INTO foo_table (foo,foo2,foo3,foo4) VALUES (:foo,:foo2,:foo3,:foo4)"
      end

      it 'should insert each data hash using all given field names' do
        @dummy_db.should_receive(:execute).with(@expected_query_1, @data[0])
        @dummy_db.should_receive(:execute).with(@expected_query_2, @data[1])

        @connection.save_data(@unique_keys, @data, 'foo_table')
      end

      context 'and datum is a single hash' do
        it 'should save the hash' do
          @expected_query_1 = "INSERT OR REPLACE INTO foo_table (foo,foo2,foo3) VALUES (:foo,:foo2,:foo3)"
          @dummy_db.should_receive(:execute).with(@expected_query_1, @data.first)
          @connection.save_data(@unique_keys, @data.first, 'foo_table')
        end
      end

      context 'and table does not exist' do
        before do
          @dummy_db.stub(:execute) # default
          # raise just once
          @dummy_db.should_receive(:execute).with(@expected_query_1, @data[0]).
                    and_raise(SQLite3::SQLException.new("no such table: foo_table") )
        end

        it 'should create table using all field names and unique keys' do
          @connection.should_receive(:create_table).with('foo_table', [:foo,:foo2,:foo3, :foo4], @unique_keys)
          @connection.save_data(@unique_keys, @data, 'foo_table')
        end

        it 'should insert data' do
          @connection.stub(:create_table)
          @dummy_db.should_receive(:execute).with(@expected_query_2, @data[1])

          @connection.save_data(@unique_keys, @data, 'foo_table')
        end
      end

      context 'and some columns do not exist' do
        before do
          @dummy_db.stub(:execute) # default
          # raise just once
          @dummy_db.should_receive(:execute).with(@expected_query_1, @data[0]).
                    and_raise(SQLite3::SQLException.new("table mynewtable has no column named foo") )
        end

        it 'should create missing fields using all field names and unique keys' do
          @connection.should_receive(:add_columns).with('foo_table', [:foo,:foo2,:foo3, :foo4])
          @connection.save_data(@unique_keys, @data, 'foo_table')
        end

        it 'should insert data' do
          @connection.stub(:add_columns)
          @dummy_db.should_receive(:execute).with(@expected_query_2, @data[1])

          @connection.save_data(@unique_keys, @data, 'foo_table')
        end
      end

      context 'and some other error is raised' do
        before do
          @dummy_db.stub(:execute) # default
          # raise just once
          @dummy_db.should_receive(:execute).with(@expected_query_1, @data[0]).
                    and_raise(SQLite3::SQLException.new("something else has gone wrong") )
        end

        it 'should raise error' do
          lambda { @connection.save_data(@unique_keys, @data, 'foo_table') }.should raise_error(SQLite3::SQLException)
          @connection.save_data(@unique_keys, @data, 'foo_table')
        end
      end

      context 'and data for unique keys is nil' do
        it 'should raise DatabaseError' do
          @data.first[:foo2] = nil
          lambda { @connection.save_data(@unique_keys, @data, 'foo_table') }.should raise_error(SqliteMagic::DatabaseError, /unique key.*foo2/)
        end
      end
    end

    describe '#create_table' do
      it 'should create default table using given field names' do
        expected_query = "CREATE TABLE some_table (foo,bar,baz)"
        @dummy_db.should_receive(:execute).with(expected_query)
        @connection.create_table(:some_table, [:foo,:bar,:baz])
      end

      context 'and unique keys are given' do
        it 'should add constraint and index for given keys' do
          expected_query_1 = "CREATE TABLE some_table (foo,bar,baz, UNIQUE (foo,baz))"
          expected_query_2 = "CREATE UNIQUE INDEX IF NOT EXISTS foo_baz ON some_table (foo,baz)"
          @dummy_db.should_receive(:execute).with(expected_query_1)
          @dummy_db.should_receive(:execute).with(expected_query_2)
          @connection.create_table(:some_table, [:foo,:bar,:baz], [:foo,:baz])
        end
      end
    end

    describe "add_columns" do
      before do
        @table_info = [{"cid"=>0, "name"=>"bar", "type"=>"", "notnull"=>0, "dflt_value"=>nil, "pk"=>0},
                       {"cid"=>1, "name"=>"rssd_id", "type"=>"", "notnull"=>0, "dflt_value"=>nil, "pk"=>0}]
      end

      it 'should get table info' do
        @dummy_db.stub(:execute)
        @dummy_db.should_receive(:table_info).with(:foo_table).and_return(@table_info)
        @connection.add_columns(:foo_table, [:foo,:bar,:baz])
      end

      it 'should add columns that arent there already' do
        @dummy_db.stub(:table_info).and_return(@table_info)
        @dummy_db.should_receive(:execute).with('ALTER TABLE foo_table ADD COLUMN foo')
        @dummy_db.should_receive(:execute).with('ALTER TABLE foo_table ADD COLUMN baz')
        @connection.add_columns(:foo_table, [:foo,:bar,:baz])
      end
    end

    describe '#insert_or_update' do
      before do
        @datum = {:foo => 'bar', :foo2 => 'bar2', :foo3 => 'bar3', :foo4 => 'bar4'}
        @unique_keys = [:foo2,:foo3]
        @expected_query = "INSERT INTO foo_table (foo,foo2,foo3,foo4) VALUES (:foo,:foo2,:foo3,:foo4)"
      end

      it 'should insert data' do
        @dummy_db.should_receive(:execute).with(@expected_query, @datum)
        @connection.insert_or_update(@unique_keys, @datum, 'foo_table')
      end

      it 'should not update data' do
        @dummy_db.should_not_receive(:execute).with(/UPDATE/, anything)
        @connection.insert_or_update(@unique_keys, @datum, 'foo_table')
      end

      context 'and no table name given' do
        before do
          @expected_query = "INSERT INTO main_table (foo,foo2,foo3,foo4) VALUES (:foo,:foo2,:foo3,:foo4)"
        end

        it 'should use main_table table by default' do
          @dummy_db.should_receive(:execute).with(@expected_query, @datum)
          @connection.insert_or_update(@unique_keys, @datum)
        end
      end

      context 'and data already exists' do
        before do
          @dummy_db.stub(:execute).with(/INSERT/, anything).and_raise(SQLite3::ConstraintException.new('constraint failed'))
          @expected_update_query = "UPDATE foo_table SET foo=:foo, foo4=:foo4 WHERE foo2=:foo2 AND foo3=:foo3"
        end

        it 'should update given columns dependent on unique keys' do
          @dummy_db.should_receive(:execute).with(@expected_update_query, @datum)
          @connection.insert_or_update(@unique_keys, @datum, 'foo_table')
        end

        context "and :update_unique_keys specified in opts" do
          it 'should update all columns including unique keys' do
            @expected_update_query = "UPDATE foo_table SET foo=:foo, foo2=:foo2, foo3=:foo3, foo4=:foo4 WHERE foo2=:foo2 AND foo3=:foo3"
            @dummy_db.should_receive(:execute).with(@expected_update_query, @datum)
            @connection.insert_or_update(@unique_keys, @datum, 'foo_table', :update_unique_keys => true)
          end
        end

        context 'and no table name given' do
          before do
            @expected_update_query = "UPDATE main_table SET foo=:foo, foo4=:foo4 WHERE foo2=:foo2 AND foo3=:foo3"
          end

          it 'should use main_table table by default' do
            @dummy_db.should_receive(:execute).with(@expected_update_query, @datum)
            @connection.insert_or_update(@unique_keys, @datum)
          end
        end

        context 'and some columns do not exist' do
          before do
            @dummy_db.should_receive(:execute).with(@expected_query, @datum).
                      and_raise(SQLite3::SQLException.new("table mynewtable has no column named foo") )
            @expected_update_query = "UPDATE foo_table SET foo=:foo, foo4=:foo4 WHERE foo2=:foo2 AND foo3=:foo3"
          end

          it 'should create missing fields using all field names and unique keys' do
            @connection.should_receive(:add_columns).with('foo_table', [:foo,:foo2,:foo3, :foo4])
            @dummy_db.stub(:execute).with(@expected_update_query, @datum)

            @connection.insert_or_update(@unique_keys, @datum, 'foo_table')
          end

          it 'should update data' do
            @connection.stub(:add_columns)
            @dummy_db.should_receive(:execute).with(@expected_update_query, @datum)

            @connection.insert_or_update(@unique_keys, @datum, 'foo_table')
          end
        end

      end

      # context 'and SQLite3::SQLException raised' do
      #   before do
      #     @dummy_db.stub(:execute) # default
      #     # raise just once
      #     @dummy_db.should_receive(:execute).
      #               and_raise(SQLite3::SQLException )
      #   end
      #
      #   it 'should defer to save_data' do
      #     @connection.stub(:create_table)
      #     @connection.should_receive(:save_data).with(@unique_keys, @datum, 'foo_table')
      #
      #     @connection.insert_or_update(@unique_keys, @datum, 'foo_table')
      #   end
      # end
    end

    describe '#verbose?' do
      it 'should return false if ENV["VERBOSE"] not set' do
        @connection.verbose?.should be_falsy
      end

      it 'should return true if ENV["VERBOSE"] set' do
        ENV["VERBOSE"] = 'foo'
        @connection.verbose?.should be_truthy
        ENV["VERBOSE"] = nil # reset
      end
    end

    describe '#close' do
      it 'should close database' do
        @dummy_db.should_receive(:close)
        @connection.close
      end
    end

    describe '#commit' do
      it 'should send commit to database' do
        @dummy_db.should_receive(:commit)
        @connection.commit
      end
    end
  end

  # before do
  #   @dummy_db = stub('database_connection')
  #   SqliteMagic.stub(:database).and_return(@dummy_db)
  # end


  # describe '#database_file' do
  #   it 'should return file based on class and in db directory in working directory' do
  #     expected_file = File.expand_path(File.join(File.dirname(__FILE__),'..','db','foobot.db'))
  #     File.expand_path(SqliteMagic.database_file).should == expected_file
  #   end
  # end

  # end
  #
  # describe '#unlock_database' do
  #   it "should start and end transaction on database" do
  #     @dummy_db.should_receive(:execute).with('BEGIN TRANSACTION; END;')
  #     SqliteMagic.unlock_database
  #   end
  # end
  #

end
