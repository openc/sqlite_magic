require 'sqlite_magic/version'
require 'sqlite3'

module SqliteMagic

  extend self

  class Error            < StandardError;end
  class DatabaseError    < Error;end
  class NoSuchTable      < Error;end

  class Connection
    attr_reader :database
    def initialize(db_loc='sqlite.db', options={})
      busy_timeout = options.delete(:busy_timeout)
      @database = SQLite3::Database.new(db_loc, options)
      @database.busy_timeout = busy_timeout if busy_timeout
    end

    def add_columns(tbl_name, col_names)
      existing_cols = database.table_info(tbl_name).map{ |c| c['name'] }
      missing_cols = col_names.map(&:to_s) - existing_cols
      missing_cols.each do |col_name|
        database.execute("ALTER TABLE #{tbl_name} ADD COLUMN #{col_name}")
      end
    end

    def close
      database.close
    end

    def commit
      database.commit
    end

    def create_table(tbl_name, col_names, unique_keys=nil)
      puts "Now creating new table: #{tbl_name}" if verbose?
      query = unique_keys ? "CREATE TABLE #{tbl_name} (#{col_names.join(',')}, UNIQUE (#{unique_keys.join(',')}))" :
                            "CREATE TABLE #{tbl_name} (#{col_names.join(',')})"
      database.execute query
      if unique_keys && !unique_keys.empty?
        query = "CREATE UNIQUE INDEX IF NOT EXISTS #{unique_keys.join('_')} " +
          "ON #{tbl_name} (#{unique_keys.join(',')})"
        database.execute query
      end
    end

    def execute(query,data=nil)
      raw_response = data ? database.execute2(query, data) : database.execute2(query)
      keys = raw_response.shift # get the keys
      raw_response.map{|e| Hash[keys.zip(e)] }
    rescue SQLite3::SQLException => e
      puts "Exception (#{e.inspect}) raised" if verbose?
      case e.message
      when /no such table/
        raise NoSuchTable.new(e.message)
      else
        raise e
      end
    end

    # This is an (expensive) convenience method to insert a row (for given unique keys), or if the row already exists
    #
    def insert_or_update(uniq_keys, values_hash, tbl_name='main_table', opts={})
      all_field_names = values_hash.keys
      field_names_as_symbol_string = all_field_names.map{ |k| ":#{k}" }.join(',') # need to appear as symbols
      sql_statement = "INSERT INTO #{tbl_name} (#{all_field_names.join(',')}) VALUES (#{field_names_as_symbol_string})"
      database.execute(sql_statement, values_hash)
    rescue SQLite3::ConstraintException => e
      unique_key_constraint = uniq_keys.map { |k| "#{k}=:#{k}" }.join(' AND ')
      update_keys = values_hash.keys
      update_keys -= uniq_keys if !opts[:update_unique_keys]
      update_sql = update_keys.map { |k| "#{k}=:#{k}" }.join(', ')
      sql_statement = "UPDATE #{tbl_name} SET #{update_sql} WHERE #{unique_key_constraint}"
      database.execute sql_statement, values_hash
    rescue SQLite3::SQLException => e
      puts "Exception (#{e.inspect}) raised" if verbose?
      case e.message
      when /no such table/
        create_table(tbl_name, all_field_names, uniq_keys)
        retry
      when /has no column/
        add_columns(tbl_name, all_field_names)
        retry
      else
        raise e
      end
    end

    # #save data into the database
    def save_data(uniq_keys, values_array, tbl_name)
      values_array = [values_array].flatten(1) # coerce to an array
      all_field_names = values_array.map(&:keys).flatten.uniq
      all_field_names_as_string = all_field_names.join(',')
      all_field_names_as_symbol_string = all_field_names.map{ |k| ":#{k}" }.join(',') # need to appear as symbols
      begin
        values_array.each do |values_hash|
          # mustn't use nil value in unique value due to fact that SQLite considers NULL values to be different from
          # each other in UNIQUE indexes. See http://www.sqlite.org/lang_createindex.html
          raise DatabaseError.new("Data has nil value for unique key. Unique keys are #{uniq_keys}. Offending data: #{values_hash.inspect}") unless uniq_keys.all?{ |k| values_hash[k] }
          sql_query =  "INSERT OR REPLACE INTO #{tbl_name} (#{all_field_names_as_string}) VALUES (#{all_field_names_as_symbol_string})"
          database.execute(sql_query, values_hash)
        end
      rescue SQLite3::SQLException => e
        puts "Exception (#{e.inspect}) raised" if verbose?
        case e.message
        when /no such table/
          create_table(tbl_name, all_field_names, uniq_keys)
          retry
        when /has no column/
          add_columns(tbl_name, all_field_names)
          retry
        else
          raise e
        end
      end
    end

    # Convenience method that returns true if VERBOSE environmental variable set (at the moment whatever it is set to)
    def verbose?
      ENV['VERBOSE']
    end

    # def buildinitialtable(data)
    #   raise "buildinitialtable: no swdatakeys" unless @swdatakeys.length == 0
    #   coldef = self.newcolumns(data)
    #   raise "buildinitialtable: no coldef" unless coldef.length > 0
    #   # coldef = coldef[:1]  # just put one column in; the rest could be altered -- to prove it's good
    #   scoldef = coldef.map { |col| format("`%s` %s", col[0], col[1]) }.join(",")
    #   @db.execute(format("create table main.`%s` (%s)", @swdatatblname, scoldef))
    # end
    # def addnewcolumn(k, vt)
    #   @db.execute(format("alter table main.`%s` add column `%s` %s", @swdatatblname, k, vt))
    # end
    # Internal function to check a row of data, convert to right format
    # def ScraperWiki._convdata(unique_keys, scraper_data)
    #     if unique_keys
    #         for key in unique_keys
    #             if !key.kind_of?(String) and !key.kind_of?(Symbol)
    #                 raise 'unique_keys must each be a string or a symbol, this one is not: ' + key
    #             end
    #             if !scraper_data.include?(key) and !scraper_data.include?(key.to_sym)
    #                 raise 'unique_keys must be a subset of data, this one is not: ' + key
    #             end
    #             if scraper_data[key] == nil and scraper_data[key.to_sym] == nil
    #                 raise 'unique_key value should not be nil, this one is nil: ' + key
    #             end
    #         end
    #     end
    #
    #     jdata = { }
    #     scraper_data.each_pair do |key, value|
    #         raise 'key must not have blank name' if not key
    #
    #         key = key.to_s if key.kind_of?(Symbol)
    #         raise 'key must be string or symbol type: ' + key if key.class != String
    #         raise 'key must be simple text: ' + key if !/[a-zA-Z0-9_\- ]+$/.match(key)
    #
    #         # convert formats
    #         if value.kind_of?(Date)
    #             value = value.iso8601
    #         end
    #         if value.kind_of?(Time)
    #           # debugger
    #             value = value.utc.iso8601
    #             raise "internal error, timezone came out as non-UTC while converting to SQLite format" unless value.match(/([+-]00:00|Z)$/)
    #             value.gsub!(/([+-]00:00|Z)$/, '')
    #         end
    #         if ![Fixnum, Float, String, TrueClass, FalseClass, NilClass].include?(value.class)
    #             value = value.to_s
    #         end
    #
    #         jdata[key] = value
    #     end
    #     return jdata
    # end
  end


  # When deciding on the location of the SQLite databases we need to set the directory relative to the directory
  # of the file/app that includes the gem, not the gem itself.
  # Doing it this way, and setting a class variable feels ugly, but this appears to be difficult in Ruby, esp as the
  # file may ultimately be called by another process, e.g. the main OpenCorporates app or the console, whose main
  # directory is unrelated to where the databases are stored (which means we can't use Dir.pwd etc). The only time
  # we know about the directory is when the module is called to extend the file, and we capture that in the
  # @app_directory class variable
  # def self.extended(obj)
  #   path, = caller[0].partition(":")
  #   @@app_directory = File.dirname(path)
  # end

end
