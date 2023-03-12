# compile
```bash
mvn clean package
```


# execution
```bash
export s3bucktname_prod="mindera-mlops-prod-bucket"
export dbhost="localhost"
export dbport=5432
export database=postgres
export dbuser=postgres
export dbpassword=******
java -jar sample-0.1-jar-with-dependencies.jar
```
