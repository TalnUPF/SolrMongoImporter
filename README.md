# Solr Mongo Importer
Welcome to the Solr Mongo Importer project. This project provides MongoDb support for the Solr Data Import Handler.
This is based on James' project (https://github.com/james75) with some improvement, mainly 
* make deltaImportQuery and and deletedPkQuery work with last_index_time
* support nested object

## Features
* Retrive data from a MongoDb collection
* Authenticate using MongoDb authentication
* Map Mongo fields to Solr fields
* Addition features (i.e. my contribution)
    * Delta import is available and works with last_index_itme
    * deletedPkQuery is available and works with last_index_itme
    * Support nested object
```json
//when innerObject is a nested object, for example
{
  "postId": "string",
  "categoryPath": "string",
  "title": "string",
  "price": {
    "value": 0,
    "currency": "USD",
    "currencySymbol": "$"
  }
}
The innerObj "price" is actually a nested object, which is not supported by Solr directly. 
We need to flatten it to key/value pairs. A simple way is to add an underscore for sub-key, 
then use the same naming convention in Solr's schema file, so the above structure will become
  price_value=0
  price_currency=USD
  price_currencySymbol=$
For details, refer to MongoDataSource.java
```
* when there are arrays there are many fields with the same name but ending with __0 __1 __2 ... despite the level the array is or even if there are several arrays  in the object embedeness


## Classes

* MongoDataSource - Provides a MongoDb datasource
    * database  (**required**) - The name of the data base you want to connect to
    * clientUri (**required** - simple example: mongodb://localhost:27017  complicated example: mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]])
* MongoEntityProcessor - Use with the MongoDataSource to query a MongoDb collection
    * collection (**required**)
    * query (**required**)
    * deltaQuery (*optional*)
    * deltaImportQuery (*optional*)
* MongoMapperTransformer - Map MongoDb fields to your Solr schema
    * mongoField (**required**)

## Preparation
1. Firstly you will need a copy of the Solr Mongo Importer jar.
    ### Getting Solr Mongo Importer
    1. [Download the latest JAR from github](https://github.com/harvshen/SolrMongoImporter/tree/master/releases)
    2. Build your own using the ant build script you will need the JDK installed as well as Ant and Ivy, just run "ant build"
2. You will also need the [Mongo Java driver 2.x JAR] (https://github.com/mongodb/mongo-java-driver/downloads), note it's not 3.x
   Or find the [local copy here] (https://github.com/harvshen/SolrMongoImporter/tree/master/releases)

3. Place both of these jar's in your Solr libaries folder ( I put mine in 'dist' folder with the other jar's)
4. Add lib directives to your solrconfig.xml

```xml
    <lib path="../../dist/mongo-java-driver-2.14.1.jar" />
    <lib path="../../dist/solr-mongo-importer-1.1.0.jar" />
```

##Step-by-step instruction
#### Step 1. Understand the DataImportHandler
https://wiki.apache.org/solr/DataImportHandler
This wiki explains it pretty well when working with relational database.

#### Step 2. Make sure your MongoDB is working
Assume my Mongodb database name "posts", the collection name "sellposts"
db.sellposts.find()
```json
/* 1 */
{
    "_id" : "2bd571b04f374d71929560d04b58ba51",
    "categoryPath" : "/SALE/Appliances",
    "title" : "string",
    "price" : {
        "value" : 123456789.88,
        "currency" : "CAD",
        "currencySymbol" : "$"
    }
}

/* 2 */
{
    "_id" : "5d55c86945004dd79a4333bf2bcc6d83",
    "categoryPath" : "/SALE/Appliances",
    "title" : "Whrilpool cabrio set",
    "price" : {
        "value" : 629.0,
        "currency" : "USD",
        "currencySymbol" : "$"
    }
}
```

#### Step 3. Declare Solr fields in schema.xml

* the definition of the dynamic field is used when there are arrays and there are several fields with the same key but ending with __0 __1... (the first one does not include any numbering) 

```xml
    <!-- Sample Solr schema.xml -->
  <fields>    
	<field name="postId" type="string" indexed="true" required="true" />
	<field name="categoryPath" type="string" indexed="true" stored="true"/>
    <field name="title" type="textnosynonym" indexed="true" stored="true" />
    <field name="price"  type="double" indexed="true" stored="true" />
    <field name="price_currency"  type="string" indexed="true" stored="true" />
    <field name="price_currencySymbol"  type="string" indexed="false" stored="true" />
    
    <dynamicField name="entities_hashtags_text*" type="ignored" />
    <field indexed="true" multiValued="true" name="hashtags" required="false" stored="true" type="lowercase"/>
    <copyField dest="hashtags" source="entities_hashtags_text*"/>
 

 </fields>
```

#### Step 4. Declare dih-config.xml in solrconfig.xml
```xml
  <config>
    ...
	<requestHandler name="/dataimport" class="org.apache.solr.handler.dataimport.DataImportHandler">
	  <lst name="defaults">
		<str name="config">dih-config.xml</str>
	  </lst>
	</requestHandler>
	...
  </config>
```

#### Step 5. Define the dih-config.xml under your Solr collection/conf folder (where schema.xml, solrconfig.xml is stored) 

* name is very important you can have things like this...

```xml
<?xml version="1.0" encoding="utf-8" ?>
<dataConfig>
    <!--clientUri=mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]] -->
    <dataSource name="MongoDS" type="MongoDataSource" database="posts" clientUri="mongodb://localhost:27017"/>
    <document name="import">
        <!-- if query="" then it imports everything -->
        <entity  processor="MongoEntityProcessor"
                 collection="sellposts"
                 datasource="MongoDS"
                 transformer="MongoMapperTransformer" name="sellpost"
                 query="{$or : [{'status' : 'AVAILABLE'},{'status' : 'SOLD'} ]}"
                 deltaQuery="{$and : [ {$or : [{'status' : 'AVAILABLE'},{'status' : 'SOLD'} ]}, {'modifiedAt':{$gt:{$date:'${dih.last_index_time}'}} } ] }"
                 deltaImportQuery="{'_id':'${dih.delta._id}'}"
                 deletedPkQuery="{$and : [ {$or : [{'status' : 'DELETED'},{'status' : 'UNLISTED'} ]}, {'modifiedAt':{$gt:{$date:'${dih.last_index_time}'}} } ] }"
                >

            <!--  If mongoField name and the field declared in schema.xml are the same, then you don't need to declare below field mapping.
                  If not same than you have to refer the mongoField to field in schema.xml
                 ( Ex: mongoField="EmpNumber" to name="EmployeeNumber").
                 <field column="EmpNumber" name="EmployeeNumber" mongoField="EmpNumber"/>
                 -->
            <field column="_id" name="postId"/>
            <field column="price_value" name="price"/>

             <field column="entities_hashtags_text"     name="hashtags"  />
             <!-- you can avoid this part -->
             <field column="entities_hashtags_text__0"     name="hashtags"  />
             <field column="entities_hashtags_text__1"     name="hashtags"  />
             <field column="entities_hashtags_text__2"     name="hashtags"  />
             <field column="entities_hashtags_text__3"     name="hashtags"  />
             <field column="entities_hashtags_text__4"     name="hashtags"  />
             <field column="entities_hashtags_text__5"     name="hashtags"  />
             <field column="entities_hashtags_text__6"     name="hashtags"  />
             <field column="entities_hashtags_text__7"     name="hashtags"  />
             <field column="entities_hashtags_text__8"     name="hashtags"  />
             <field column="entities_hashtags_text__9"     name="hashtags"  />
             <field column="entities_hashtags_text__10"     name="hashtags"  />
             <field column="entities_hashtags_text__11"     name="hashtags"  />
             <field column="entities_hashtags_text__12"     name="hashtags"  />
             <field column="entities_hashtags_text__13"     name="hashtags"  />
             <field column="entities_hashtags_text__14"     name="hashtags"  />
             <field column="entities_hashtags_text__15"     name="hashtags"  />
             <field column="entities_hashtags_text__16"     name="hashtags"  />


        </entity>
    </document>
</dataConfig>

```

#### Step 6. Run the full import
Assuming solr is running on port 8080 and mongodb are running on 27017, 
open the following link http://localhost:8983/solr/sellpost/dataimport?command=full-import 
This should trigger the full index to import the data from mongodb to solr.

Try the search query: http://localhost:8983/solr/sellpost/query?q=*

#### Step 7. Enable the delta import scheduler job
If you need auto scheduling job configured for the delta import job, you can [find more details here] (https://wiki.apache.org/solr/DataImportHandler)
For your convenience, I also include the source code

* SolrDataImportProperties.java
* ApplicationListener.java
* HTTPPostScheduler.java

##### Step 7.1 compile the above source and put it in the classpath
##### Step 7.2 declear it in web.xml
```xml
 <!-- web.xml -->
   <listener>
     <listener-class>org.apache.solr.handler.dataimport.scheduler.ApplicationListener</listener-class>
   </listener>
```
##### Step 7.3 define dataimport.properties
${solrHome}/solr/conf/dataimport.properties
```
#################################################
# delta dataimport scheduler properties         #
#################################################
#  to sync or not to sync
#  1 - active; anything else - inactive
syncEnabled=1

#  which cores to schedule
#  in a multi-core environment you can decide which cores you want syncronized
#  leave empty or comment it out if using single-core deployment
syncCores=sellpost

#  solr server name or IP address
#  [defaults to localhost if empty]
server=127.0.0.1

#  solr server port
#  [defaults to 80 if empty]
port=8983

#  application name/context
#  [defaults to current ServletContextListener's context (app) name]
webapp=solr

#  URL params [mandatory]
#  delta import command remainder of URL
params=/dataimport?command=delta-import&clean=false&commit=true

#  define how frequent the delta import should run
#  (number of minutes between two runs)
#  [defaults to 10 if empty]
interval=5
```
