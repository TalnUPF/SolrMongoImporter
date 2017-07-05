package org.apache.solr.handler.dataimport;


import com.mongodb.*;
import com.mongodb.util.JSON;
// import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

/**
 * User: James
 * Date: 13/08/12
 * Time: 18:28
 * To change this template use File | Settings | File Templates.
 */


public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>>{

    private static final Logger LOG = LoggerFactory.getLogger(TemplateTransformer.class);

    private DBCollection mongoCollection;
    private DB           mongoDb;
    private Mongo        mongoConnection;

    private DBCursor mongoCursor;
// this makes it not reentrant, 
    Map<String, Object> result ;

    @Override
    public void init(Context context, Properties initProps) {
        String databaseName   = initProps.getProperty( DATABASE   );
        String host           = initProps.getProperty( HOST, "localhost" );
        String port           = initProps.getProperty( PORT, "27017"     );
        String username       = initProps.getProperty( USERNAME  );
        String password       = initProps.getProperty( PASSWORD  );

        if( databaseName == null ) {
            throw new DataImportHandlerException( SEVERE  , "Database must be supplied");
        }

        try {
            Mongo mongo  = new Mongo( host, Integer.parseInt( port ) );
            mongo.setReadPreference(ReadPreference.secondaryPreferred());

            this.mongoConnection = mongo;
            this.mongoDb = mongo.getDB(databaseName);

            if( username != null ){
                if( this.mongoDb.authenticate( username, password.toCharArray() ) == false ){
                    throw new DataImportHandlerException( SEVERE, "Mongo Authentication Failed");
                }
            }

        } catch ( UnknownHostException e ) {
            throw new DataImportHandlerException( SEVERE  , "Unable to connect to Mongo");
        }
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {
        LOG.info("Executing MongoQuery: " + query.toString());
        DBObject queryObject  = (DBObject) JSON.parse( query );

        long start = System.currentTimeMillis();
        mongoCursor  = this.mongoCollection.find( queryObject );
        LOG.trace("Time taken for mongo :"
                + (System.currentTimeMillis() - start));

        ResultSetIterator resultSet = new ResultSetIterator( mongoCursor );
        return resultSet.getIterator();
    }

    public Iterator<Map<String, Object>> getData(String query, String collection ) {
        this.mongoCollection = this.mongoDb.getCollection( collection );
        return getData(query);
    }

    private class ResultSetIterator {
        DBCursor MongoCursor;

        Iterator<Map<String, Object>> rSetIterator;

        public ResultSetIterator( DBCursor MongoCursor ){
            this.MongoCursor = MongoCursor;


            rSetIterator = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return hasnext();
                }

                public Map<String, Object> next() {
                    return getARow();
                }

                public void remove() {/* do nothing */
                }
            };


        }

        public Iterator<Map<String, Object>> getIterator(){
            return rSetIterator;
        }

      /**  the original one does not expect to have complex objects
      * an object has fields which can be: single values, other objects and arrays
      * And the current implementations seems to deal only with two levels of objects.
      * So the we should do recursive calls that deals with a single level
      * It can be an array, a single object or a complex object
      * If it is a single object adds it to the Map, 
      * If it is an object calls for each member adding the name to the key
      * If it is an array
      * the most problematic ones are arrays that need to be imported in solr as multivalued, and as result is a Map, does not allow duplicated names.
      * one way to solve it is to detect duplicated keys and when we have a duplicated key we generate a new key with a number at the end like __1 __2.... so 
      * 
      *
      */
        
        
        private Map<String, Object> getARow(){
            DBObject mongoObject = getMongoCursor().next();

            result   = new HashMap<String, Object>();
            Set<String>         keys     = mongoObject.keySet();
            Iterator<String>    iterator = keys.iterator();

            while ( iterator.hasNext() ) {
                String key = iterator.next();
                Object innerObject = mongoObject.get(key);
                // key="key_"+key;
                if(innerObject instanceof BasicDBObject) {
                	subOjbect(key,(BasicDBObject)innerObject);
                }else if (innerObject instanceof ArrayList<?>) {
                	// then is an Array.class...
                   subOjbectArray(key,(ArrayList<Object>)innerObject);
                }else { //innerObject is String or other type, just add it
                    addKey(key, innerObject);
                }
            }

            return result;
        }

        /**
         * Adds a key, object pair to result.
         * If result already contains the key it adds __number at the end of the key
         * this means that array will become key, key__0, key__1,key__2....
         * @param key
         * @param innerObject
         */
        
        private void addKey(String key, Object innerObject) {
        	if (result.containsKey(key)) {
        		int counter=0;
        	while (result.containsKey(key+"__"+counter)) counter++;
        		key+="__"+counter;
        	}
        	result.put( key,  innerObject);
        	
    	}

        /**
         *  when the suboject is an array will ckech the array members and perform the call or recursive call on each object
         * @param string
         * @param innerObject
         */
        private void subOjbectArray(String key, ArrayList<Object> object) {

            for(Object innerObject: object){
                if(innerObject instanceof BasicDBObject) {
                	subOjbect(key,(BasicDBObject)innerObject);
                }else if (innerObject instanceof ArrayList<?>) {
                	// then is an Array.class...
                   subOjbectArray(key,(ArrayList<Object>)innerObject);
                }else { //innerObject is String or other type, just add it
                    addKey(key, innerObject);
                }

            }      
        }

		private void subOjbect(String key, BasicDBObject object) {

            for(String subKey : object.keySet()){
            	 Object innerObject=object.get(subKey);
                 if(innerObject instanceof BasicDBObject) {
                 	subOjbect(key+"_"+subKey,(BasicDBObject)innerObject);
                 }else if (innerObject instanceof ArrayList<?>) {
                 	// then is an Array.class...
                    subOjbectArray(key+"_"+subKey,(ArrayList<Object>)innerObject);
                 }else { //innerObject is String or other type, just add it
                     addKey(key+"_"+subKey, innerObject);
                 }
             }
	}

		private boolean hasnext() {
            if (MongoCursor == null)
                return false;
            try {
                if (MongoCursor.hasNext()) {
                    return true;
                } else {
                    close();
                    return false;
                }
            } catch (MongoException e) {
                close();
                wrapAndThrow(SEVERE,e);
                return false;
            }
        }

        private void close() {
            try {
                if (MongoCursor != null)
                    MongoCursor.close();
            } catch (Exception e) {
                LOG.warn("Exception while closing result set", e);
            } finally {
                MongoCursor = null;
            }
        }
    }

    private DBCursor getMongoCursor(){
        return this.mongoCursor;
    }


	@Override
    public void close() {
        if( this.mongoCursor != null ){
            this.mongoCursor.close();
        }

        if (this.mongoConnection !=null ){
            this.mongoConnection.close();
        }
    }


    public static final String DATABASE   = "database";
    public static final String HOST       = "host";
    public static final String PORT       = "port";
    public static final String USERNAME   = "username";
    public static final String PASSWORD   = "password";

}

