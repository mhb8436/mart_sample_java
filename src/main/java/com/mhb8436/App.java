package com.mhb8436;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.sql.*;
import java.util.*;

/**
 * compile : mvn clean package
 * run : java -jar target/sample-0.1-jar-with-dependencies.jar mindera-mlops-prod-bucket mart_sample/AN10001/
 */
public class App 
{
    private static AmazonS3 s3;
    
    public static void main( String[] args )
    {   
        System.out.println("App begin");
        if (args.length < 2) {
            System.out.format("Usage: <the bucket name> <the Input Directory >\n" +
                    "Example: java -jar xxx.jar mart_sample/AN10001/\n");
            return;
        }
        // 아마존 버켓이름을 환경변수에서 가져온다.
        String bucket_name = System.getenv("s3bucktname_prod");
        String path = args[0]; // 작업용 파일 명을 실행 파라미터에서 가져온다. 
        String odate = args[1]; // YYYYMMDD 포멧의 현재날짜를 실행 파라미터에서 가져온다. 
        try{            
            s3 = AmazonS3ClientBuilder.standard()                
                .withCredentials(new EnvironmentVariableCredentialsProvider())
                .withRegion(Regions.DEFAULT_REGION)
                .build();
        }catch(Exception e){
            System.err.println("s3 build err => " + e.getMessage());
        }
        
        // ListMyBuckets();
        String[] filename = {"A.txt", "B.txt", "C.txt"}; // 작업용 파일들
        
        Map<String, Integer> wordCountMap = new HashMap();
        try{
            for(String name: filename) {
                String key_name = path + name; // S3 경로와 파일명을 붙여서 키를 만든다. 
                String content = getContent(bucket_name, key_name); // 해당 버킷명과 키 명으로 파일 내용을 읽는다. 
                wordCountMap.put(odate + "_" + name, content.split(" ").length); // 현재날짜_파일명, 워드카운트갯수
            }
        }catch(Exception e){
            System.err.println("filename : " + filename + " get err =>" + e.getMessage());
        }
        
        saveToDb(wordCountMap); // 디비에 작업된 워드카운트를 넣는다. 
    }
    /**
     * 버킷명과 키명으로 S3의 파일을 읽는다. 
     * @param bucket_name
     * @param key_name
     * @return
     */
    private static String getContent(String bucket_name, String key_name){
        String content = "";
        try{
            S3Object o = s3.getObject(bucket_name, key_name);
            BufferedReader reader = new BufferedReader(new InputStreamReader(o.getObjectContent()));
            content = reader.readLine();            
        } catch(Exception e){
            
        }
        return content;
    }
    /**
     * postgres 데이터베이스에 매개변수로 주어진 데이터를 넣는다. 
     * postgres의 host, port, database, user, password는 환경변수에서 읽는다. 
     * @param data
     * @return
     */
    private static boolean saveToDb(Map<String, Integer> data){
        String url = "jdbc:postgresql://" + System.getenv("dbhost") + ":" + System.getenv("dbport") + "/" + System.getenv("database");
        String user = System.getenv("dbuser");
        String password = System.getenv("dbpassword");
        Connection conn = null;
        Statement st = null;
        try{
            conn = DriverManager.getConnection(url, user, password);
            if(conn == null){
                System.out.println("Connection Fail");             
            }
            st = conn.createStatement();
            String create_sample_table = "create table if not exists wordcount(name varchar(255), count int)";
            st.executeUpdate(create_sample_table); // wordcount 테이블을 생성한다. 

            StringBuilder sb = new StringBuilder();
            for(String key: data.keySet()){
                sb.append(String.format("insert into wordcount values('%s', %d);", key, data.get(key)));
            }
            st.executeUpdate(sb.toString()); // 매개변수로 주어진 데이터를 wordcount에 넣는다. 

            String query1 = "select * from wordcount";
            ResultSet result_query1 = st.executeQuery(query1); //  wordcount 테이블의 데이터를 조회한다.
            System.out.println("Name\tCount");
            while(result_query1.next()){
                String name = result_query1.getString("name");
                int count = result_query1.getInt("count");
                System.out.println(name + "\t" + count ); // 콘솔 출력으로 해당 row 데이터를 내보낸다. 추후 이 콘솔의 출력은 snakemake에서 지정한 outputs_folder내에 쓰여진다.
            }
        }catch(Exception e){
            System.err.println("saveDB err => " + e.getMessage());
            return false;
        }finally{
            try{
                if(st != null) st.close();
                if(conn != null) conn.close();
            }catch(Exception e){
                return false;
            }
        }
        return true;
    }

}
