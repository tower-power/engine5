/* Database Schema for Energy Management Cloud
 *
 * Version 3
 */

/* 
 * Basics 
 */
/* UUIDs */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS nodes;

/* 
 * Base table structure to serve as template
 * 
 * The assumption is that every object has a URL as a binary key and in parallel
 * it is uniquely described by a clockid and a transactional sequence number, 
 * which give it a spatial and timely coordinate
 *
 * We further assume that the data, which describe the object can be encoded using 
 * JSON.
 *
 * (To be discussed: do we need a UUID to identify the object in parallel)
 */
CREATE TABLE nodes.base (
    id      uuid,
    url     text,  
    data    json,
    clockid bigint,
    tsn     bigint,
    primary key(url),
    unique (clockid, tsn )
);
/* 
 * the table to describe all management nodes
 * 
 *(derived from base)
 */
CREATE TABLE nodes.systems ( LIKE nodes.base );

/* 
 * sequence counter (local) (managed via clockid)
 * 
 * We have not more than one sequence per schema.
 * The sequence is also to be identified by the 
 * clockid (in the nodes.systems table)
 */
CREATE SEQUENCE nodes.tsn;
CREATE SEQUENCE nodes.clockid;


/* 
 * local clock
 */
CREATE OR REPLACE FUNCTION nodes.new_tsn() RETURNS BIGINT AS $$
   BEGIN
    
     RETURN nextval( 'nodes.tsn' );

   END;
$$ LANGUAGE plpgsql;

/* 
 * Returns the id of the local 'clock'
 *
 */
CREATE OR REPLACE FUNCTION nodes.clockid() RETURNS bigint AS $$
   DECLARE 
      _clockid bigint;
   BEGIN
      _clockid = nextval( 'nodes.clockid' );
      IF _clockid = 0 THEN
        _clockid = nextval( 'nodes.clockid' );
      END IF;
      RETURN _clockid;
   END
$$ LANGUAGE plpgsql;

/*
 * Registers the local node (initial)
 */
CREATE OR REPLACE FUNCTION nodes.register( _url text, _data json) RETURNS UUID  AS $$
   DECLARE
      _uuid    uuid;
      _clockid bigint;
      old_data json;
   BEGIN
     /* do we have a clock already */
     SELECT data FROM nodes.systems WHERE url = _url INTO old_data;

     IF NOT FOUND THEN

            /* no: initialize it */
            _uuid := uuid_generate_v4();

            SELECT nodes.clockid() INTO _clockid;

            INSERT INTO nodes.systems( id, url, data, clockid, tsn  )
              VALUES ( _uuid, _url, _data, _clockid, nextval( 'nodes.tsn' ) );

     ELSE
            /* re-register and update with new tsn */
            UPDATE nodes.systems 
            SET data = _data, tsn = nextval( 'nodes.tsn' )
               WHERE url = _url;
            SELECT id FROM nodes.systems WHERE url = _url INTO _uuid;
     END IF;

     RETURN _uuid;

   END
$$ LANGUAGE plpgsql;

/*
 * High-water mark vector of all known nodes
 */
CREATE TABLE nodes.highwatermarks (
     clockid    bigint,
     tsn        bigint,
     PRIMARY KEY( clockid, tsn )
);

/* 
 * Operations Log (local)
 */
CREATE TABLE nodes.oplog (
     
     clockid    bigint,
     tsn        bigint,
     table_name text,  /* TG_TABLE_NAME */
     op         text,  /* TG_OP */
     PRIMARY KEY( clockid, tsn )
);

CREATE OR REPLACE FUNCTION onChange() RETURNS TRIGGER AS $$
     DECLARE
     BEGIN
          INSERT INTO nodes.oplog( clockid, tsn, table_name, op ) 
             VALUES (NEW.clockid, NEW.tsn, TG_TABLE_NAME, TG_OP ); 
          RETURN NEW;
     END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER onChange BEFORE INSERT OR UPDATE OR DELETE ON nodes.systems
  FOR EACH ROW EXECUTE PROCEDURE onChange();

/* 
 * read the (received) high-water marks of remote nodes
 */
CREATE OR REPLACE FUNCTION nodes.getRemoteHighs() RETURNS TABLE(  _clockid bigint, _tsn bigint ) AS $$
   BEGIN
     RETURN QUERY
        select clockid, tsn from nodes.highwatermarks;
   END
$$ LANGUAGE plpgsql;

/* 
 * get the high-water mark of the local node
 */
CREATE OR REPLACE FUNCTION nodes.getLocalHigh() RETURNS TABLE(  _clockid bigint, _tsn bigint ) AS $$
   BEGIN
     RETURN QUERY
        select clockid, currval( 'nodes.tsn') from nodes.clockid where clock = 0;
   END
$$ LANGUAGE plpgsql;

/*
 * write a new high-water mark record for a remote node
 */
CREATE OR REPLACE FUNCTION nodes.putRemoteHigh(  _clockid bigint, _tsn bigint) RETURNS VOID AS $$
   BEGIN
       LOOP
            UPDATE nodes.highwatermarks
              SET tsn = _tsn
              where clockid = _clockid;
        IF found THEN
            RETURN;
        END IF;
        BEGIN
            INSERT INTO nodes.highwatermarks( clockid, tsn ) 
            VALUES( _clockid, _tsn );
            
            EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;   
       END LOOP;
   END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION nodes.checkHigh( _clockid bigint ) RETURNS  bigint AS $$
   DECLARE 
      new_tsn bigint;
   BEGIN
      SELECT tsn from nodes.highwatermarks where clockid = _clockid
        INTO new_tsn;
      IF FOUND THEN
        RETURN new_tsn;
      ELSE
        RETURN 0;
      END IF;
   END
$$ LANGUAGE plpgsql;


