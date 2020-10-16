# 【 クラウド - オンプレ T_IJOU 連携ツール 】
# SQLServerの「T_IJOU」テーブルと、postgreSQLの「T_IJOU」テーブルを
# 比較して、差分データを、postgreSQL⇒SQLServerにInsertする処理です
# 

require 'win32ole'						# SQLserver
require 'pg'							# postgreSQL

# ---------------------------------------------------------------------------------------------
# PostgreSQL(本番)サーバーへの接続
# ---------------------------------------------------------------------------------------------
class DB_HonCon
	
  	def initialize(host, user, passwd, dbname)
  	
    	@host   = host					# ホスト名
    	@user   = user					# ユーザー名
    	@passwd = passwd				# パスワード
		@dbname = dbname				# データベース名
	end
	
	# ----------------------------------------------------------------------------------------
	# T_IJOU テーブルの読込み
	def read_IJOU
		
	    begin						
			$t_ijou_all = []
			# PostgreSQL(本番)に接続
			connection = PG::connect(:host => @host, :user => @user, :password => @passwd, :dbname => @dbname)			

			# T_IJOU テーブルを検索
			_sql = "Select * From \"T_IJOU\" Where \"F_RNO\" > #{$rno_max.to_i} ORDER BY \"F_RNO\""
			result = connection.exec(_sql)
  
			if result.ntuples > 0

				# 検索内容を配列にセット
				result.each do |_rec|
					
					@t_ijou = []
					@t_ijou << _rec['F_RNO']				# F_RNO
					@t_ijou << _rec['F_SCODE']				# F_SCODE
					@t_ijou << _rec['F_TIME']				# F_TIME
					@t_ijou << _rec['F_ALMNO']				# F_ALMNO
					@t_ijou << _rec['F_HAP']				# F_HAP
					@t_ijou << _rec['F_CORREC']				# F_CORREC
					@t_ijou << _rec['F_DEL']				# F_DEL								
					$t_ijou_all << @t_ijou					
				end				
			end			
			connection.finish
			result.ntuples
	    rescue  => ex
			print "***** " + self.class.name.to_s + "." + __method__.to_s + " *****\n"
		    print(ex.class," -> ",ex.message)
	    end
	end
end

# ---------------------------------------------------------------------------------------------
# SQLServerへの接続
# ---------------------------------------------------------------------------------------------
class DB_SQLSvr
	
	# ----------------------------------------------------------------------------------------
	# T_IJOU テーブルを読み込む （F_RNOのMAXを取得）
	def read_IJOU
				
		begin
			cn = WIN32OLE.new("ADODB.Connection")			
			cn.Open $CONNECT_SQLSERVER

			# F_IJOU テーブルからF_RNOのMAXを取得
			_sql = "Select Max(F_RNO) As F_RNO From T_IJOU Order By F_RNO DESC"

			rs = cn.Execute(_sql);
			rs.extend Recordset;
			rs.each_record { |rs| $rno_max = rs["F_RNO"] }			# F_RNOのMAXを取得
			
			cn.Close		
		rescue => ex
			print "***** " + self.class.name.to_s + "." + __method__.to_s + " *****\n"
		   	print(ex.class," -> ",ex.message)
		end
	end

	# ----------------------------------------------------------------------------------------
	# T_IJOU テーブルの更新
	def update_IJOU		

		begin			
			unless $t_ijou_all.empty?
				
				$t_ijou_all.each do | t_ijou |

					cn1 = WIN32OLE.new("ADODB.Connection")					
					cn1.Open $CONNECT_SQLSERVER

					_date = t_ijou[2].to_s.match(/[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}/)		# 日付を抽出
					_time = t_ijou[2].to_s.match(/[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}/)	# 時間を抽出				
					_date2 = "#{_date[0]}" + " " + "#{_time[0]}"
					
					# T_IJOU テーブルの新規更新
					_sql = "Insert Into T_IJOU ("
					_sql += "F_SCODE, F_TIME, F_ALMNO, F_HAP, F_CORREC, F_DEL, F_INF, F_UPDATEFLAG) Values ("					
					_sql += "#{t_ijou[1].to_i},"
					_sql += "\'#{_date2}\',"
					_sql += "#{t_ijou[3].to_i},"
					_sql += "#{t_ijou[4].to_i},"
					_sql += "#{t_ijou[5].to_i},"
					_sql += "#{t_ijou[6].to_i},"
					_sql += "\'\',"
					_sql += "0"
					_sql += ");"

					cn1.Execute(_sql);
					cn1.Close	
				end									
			end
		rescue => ex		
			print "***** " + self.class.name.to_s + "." + __method__.to_s + " *****\n"
		   	print(ex.class," -> ",ex.message)
		end
	end

	# ----------------------------------------------------------------------------------------
	# レコードセット
	module Recordset
		def [] field
			self.Fields.Item(field).Value
		end
		def []= field,value
			self.Fields.Item(field).Value = value
		end
		def each_record
			if self.EOF or self.BOF
			return 
			end
			self.MoveFirst
			until self.EOF or self.BOF
				yield self
				self.MoveNext
			end
		end
	end
end

# ===========================================================================================
# メイン処理
# ===========================================================================================
# 固定値の設定　（顧客毎に設定します）
# $CONNECT_SQLSERVER = SQLServerへの接続情報
# $DBNAME_POSTGRESQL = PostgreSQLへ接続するためのデータベース名
case ARGV[0].to_i
	when 1			# 海南市
		$CONNECT_SQLSERVER = "Provider=SQLOLEDB; Data Source=xxxxx; Initial Catalog=XXXXX; User ID=sa; Password=xx"		
		@DBNAME_POSTGRESQL = ""
		@HOSTNM_POSTGRESQL = ""		
	when 2			# 大野市
		$CONNECT_SQLSERVER = "Provider=SQLOLEDB; Data Source=xxxxx; Initial Catalog=XXXXX; User ID=sa; Password=xx"	
		@DBNAME_POSTGRESQL = ""
		@HOSTNM_POSTGRESQL = ""		
	when 3			# 郡山市
		$CONNECT_SQLSERVER = "Provider=SQLOLEDB; Data Source=xxxxx; Initial Catalog=XXXXX; User ID=sa; Password=xx"	
		@DBNAME_POSTGRESQL = ""
		@HOSTNM_POSTGRESQL = ""		
	when 0			# 社内テスト用		
		$CONNECT_SQLSERVER = "Provider=SQLOLEDB; Data Source=UGCH-004NO13; Initial Catalog=KAINAN; User ID=sa; Password=sa"		
		@DBNAME_POSTGRESQL = "oono_gesui"
		@HOSTNM_POSTGRESQL = "localhost"		
end

def syori_1
	# SQLServerへの接続
	dbs = DB_SQLSvr.new	
	dbs.read_IJOU
	
	#$rno_max = $rno_max.to_i							   	　　# テスト用ダミー	
	puts "ｵﾝﾌﾟﾚ - F_RNO 最大値 =  #{$rno_max.to_s}"	         	# RNOのMAX
end

def syori_2	
	# PostgreSQL(本番)に接続
	dbp = DB_HonCon.new(@HOSTNM_POSTGRESQL, "postgres", "sofinet", @DBNAME_POSTGRESQL)	
	$cnt_ijou = dbp.read_IJOU
	
	#puts "ｸﾗｳﾄﾞ - 更新対象件数 =  #{$cnt_ijou}"				# 連携対象のデータ件数
end

def syori_3
	if $cnt_ijou > 0 
		# SQLServerへの接続
		dbs = DB_SQLSvr.new
		dbs.update_IJOU
		_msg = "ｸﾗｳﾄﾞ - #{$t_ijou_all.size} 件更新されました"
	else
		_msg = "ｸﾗｳﾄﾞ - 更新されていません"			
	end
	puts _msg													# 更新メッセージ
end

syori_1
syori_2
syori_3

puts "------------------------------"
exit (0)