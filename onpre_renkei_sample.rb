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
    # T_IJOU テーブルからデータ件数を取得
    def read_IJOU
        
        begin
            # PostgreSQL(本番)に接続
            connection = PG::connect(:host => @host, :user => @user, :password => @passwd, :dbname => @dbname)

            # T_IJOU テーブルからデータ件数を取得
            _sql = "Select\"F_RNO\" From \"T_IJOU\""
            result = connection.exec(_sql)
            
            $count_pos = result.ntuples						# データ件数
            connection.finish
        rescue  => ex
            print "***** " + self.class.name.to_s + "." + __method__.to_s + " *****\n"
            print(ex.class," -> ",ex.message)
        end
    end

    # ----------------------------------------------------------------------------------------
    # T_IJOU テーブルの読込み
    def read_IJOU2

        begin
            $t_ijou_all = []
            # PostgreSQL(本番)に接続
            connection = PG::connect(:host => @host, :user => @user, :password => @passwd, :dbname => @dbname)

            # T_IJOU テーブルを検索
            _sql = "Select * From \"T_IJOU\" ORDER BY \"F_RNO\" DESC Limit #{$count_pos - $count_sql}"
            result = connection.exec(_sql)
            
            if result.ntuples > 0

                # 検索内容を配列にセット
                result.reverse_each do | _rec |
                    
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
    # T_IJOU テーブルからデータ件数を取得
    def read_IJOU
                
        begin
            cn = WIN32OLE.new("ADODB.Connection")
            cn.Open $CONNECT_SQLSERVER

            # F_IJOU テーブルからデータ件数を取得
            _sql = "Select Count(*) As COUNT From T_IJOU"

            rs = cn.Execute(_sql);
            rs.extend Recordset;
            rs.each_record { |rs| $count_sql = rs["COUNT"] }			# データ件数
            
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
    when 1			# ○○市
        $CONNECT_SQLSERVER = ""
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

    puts "ｵﾝﾌﾟﾚ側 -> データ件数 =  #{$count_sql}"                # データ件数
    end

    def syori_2
    # PostgreSQL(本番)に接続
    dbp = DB_HonCon.new(@HOSTNM_POSTGRESQL, "postgres", "xxxxxxxxx", @DBNAME_POSTGRESQL)	
    dbp.read_IJOU

    #$count_pos  = $count_pos.to_i + 34                        # テスト用ダミー
    puts "ｸﾗｳﾄﾞ側 -> データ件数 =  #{$count_pos}"                # データ件数
    end

def syori_3
    if $count_sql < $count_pos
        # PostgreSQL(本番)に接続
        dbp = DB_HonCon.new(@HOSTNM_POSTGRESQL, "postgres", "xxxxxxxxx", @DBNAME_POSTGRESQL)
        dbp = dbp.read_IJOU2

        # SQLServerへの接続
        dbs = DB_SQLSvr.new
        dbs.update_IJOU
        _msg = "ｵﾝﾌﾟﾚ側 -> #{$t_ijou_all.size} 件更新されました"
    else
        _msg = "ｵﾝﾌﾟﾚ側 -> 更新されていません"
    end
    puts _msg													# 更新メッセージ
end

syori_1
syori_2
syori_3

puts "------------------------------"
exit (0)