/* Database Schema for Energy Management Cloud
 *
 * Version 3
 */

/* 
 * Basics 
 */
/* UUIDs */
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
/* Crypto functions */
CREATE EXTENSION IF NOT EXISTS pgcrypto;

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
 * An object has three kidn of keys:
 * 
 *  * URL : a url to give the object a name
 *          (abrbitrary size string)
 *  * clockid/tsn : two integers, which give unique time and space coordinate
 *  *               (2 x 8 bytes)
 *  * ckey : a digest of the url as a md5 code 
 *           (16bytes)
 *
 * The value of the JSON data are as well recorded as a MD5 digest in 
 * the cval field
 * (16 bytes)
 *
 * On the Go side objects derived from nodes.base can be retrieved and managed using
 * the "Things" datastructure and package.
 *
 */
CREATE TABLE nodes.base (
    ckey    bytea,    
    cval    bytea,
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
 * clockid 
 *
 * We generate a clockid via the nodes.clockidsn sequence
 * seperately
 */
CREATE SEQUENCE nodes.tsn;
CREATE SEQUENCE nodes.clockidsn;


/* 
 * local clock
 */
CREATE OR REPLACE FUNCTION nodes.new_tsn() RETURNS BIGINT AS $$
   BEGIN
    
     RETURN nextval( 'nodes.tsn' );

   END;
$$ LANGUAGE plpgsql;

/* 
 * Returns the id of the 'clock' Sequence counter
 * (O is never returned)
 *
 */
CREATE OR REPLACE FUNCTION nodes.clockidsn() RETURNS bigint AS $$
   DECLARE 
      _clockid bigint;
   BEGIN
      _clockid = nextval( 'nodes.clockidsn' );
      /* should not be zero */
      IF _clockid = 0 THEN
        _clockid = nextval( 'nodes.clockidsn' );
      END IF;
      RETURN _clockid;
   END
$$ LANGUAGE plpgsql;

/* 
 * myClockId
 *
 * returns the clockid of the clock sequence of the local database
 *
 * This is a template function, which will be overridden after 
 * initialization.
 *
 * The clockID shoud never be zero!
 * The function is supposed to return a constant (IMMUTABLE)
 */
CREATE OR REPLACE FUNCTION nodes.myclockid() RETURNS bigint AS $$ 
      BEGIN
       RETURN 0;
      END;
$$ LANGUAGE plpgsql IMMUTABLE;

/* 
 * set my clockid
 *
 * internally called by registration to override the clockID
 */
CREATE OR REPLACE FUNCTION nodes.setmyclockid( _clockid bigint ) RETURNS VOID AS $body$ 
   DECLARE 
      sql1 text := 'CREATE OR REPLACE FUNCTION nodes.myclockid() RETURNS bigint AS $$
                   BEGIN return ';
      sql2 text := '; END;
                   $$ LANGUAGE plpgsql IMMUTABLE;';
      sql  text;
   BEGIN
     sql := sql1 || _clockid || sql2;
     EXECUTE sql;
   END;
$body$ LANGUAGE plpgsql;


/*
 * Registers the local node (initial)
 */
CREATE OR REPLACE FUNCTION nodes.register( _url text, _data json) RETURNS bigint  AS $$
   DECLARE
      _ckey    bytea;
      _cval    bytea;
      old_cval bytea;
      _clockid bigint;
      old_data json;
   BEGIN
     /* do we have a clock already */
     SELECT clockid FROM nodes.systems WHERE url = _url INTO _clockid;

     _cval = digest( _data::text, 'md5' );
     IF NOT FOUND THEN

            /* no: initialize it */
            _ckey := digest( _url, 'md5');

            SELECT nodes.clockidsn() INTO _clockid;

            INSERT INTO nodes.systems( ckey, cval, url, data, clockid, tsn  )
              VALUES ( _ckey, _cval, _url, _data, _clockid, nextval( 'nodes.tsn' ) );

     ELSE
            /* check, if changed */
            select cval from nodes.systems where url = _url into old_cval;
            if old_cval <> _cval then
              /* re-register and update with new tsn */
              UPDATE nodes.systems 
              SET cval = _cval, data = _data, tsn = nextval( 'nodes.tsn' )
                 WHERE url = _url;
            else
              select data from nodes.systems where url = _url into old_data;
              if old_data::text <> _data::text then
                UPDATE nodes.systems 
                SET cval = _cval, data = _data, tsn = nextval( 'nodes.tsn' )
                  WHERE url = _url;
              end if;
            end if;
            SELECT clockid FROM nodes.systems WHERE url = _url INTO _clockid;
     END IF;

     RETURN _clockid;

   END
$$ LANGUAGE plpgsql;

/*
 * High-water mark vector of all known nodes
 */
CREATE TABLE nodes.highwatermarks (
     clockid    bigint,
     tsn        bigint,
     PRIMARY KEY( clockid )
);

/* 
 * Operations Log (local)
 *
 * logs all change operations with clockid, tsn and tablename
 */
CREATE TABLE nodes.oplog (
     
     clockid    bigint,
     tsn        bigint,
     table_name text,  /* TG_TABLE_NAME */
     op         text,  /* TG_OP */
     PRIMARY KEY( clockid, tsn )
);

/* Trigger function
 * 
 * to be executed when changes to data happen
 */
CREATE OR REPLACE FUNCTION onChange() RETURNS TRIGGER AS $$
     DECLARE
          _opcode text;
          _clockid bigint;
          _tsn     bigint;
     BEGIN
          /* I, U or D: Insert, Update, Delete */
          _opcode = left( TG_OP , 1 ); /* first letter is enough */
         
          IF _opcode = 'D' THEN
           _clockid = nodes.myclockid();
           _tsn     = nodes.new_tsn();
          ELSE
           _clockid = NEW.clockid;
           _tsn     = NEW.tsn;
          END IF;
          INSERT INTO nodes.oplog( clockid, tsn, table_name, op ) 
               VALUES (_clockid, _tsn, TG_TABLE_NAME, _opcode );  

          UPDATE nodes.highwatermarks SET tsn = _tsn WHERE clockid = _clockid;
          IF NOT FOUND THEN
            BEGIN
              INSERT INTO nodes.highwatermarks( clockid, tsn ) 
              VALUES (_clockid, _tsn );
            EXCEPTION WHEN unique_violation THEN
              /* do nothing */
            END; 
          END IF;
          RETURN NEW;
     END;
$$ LANGUAGE plpgsql;

/* All managed tables must be isted here */
CREATE TRIGGER onChange BEFORE INSERT OR UPDATE OR DELETE ON nodes.systems
  FOR EACH ROW EXECUTE PROCEDURE onChange();

/* 
 * read the (received) high-water marks of remote nodes
 */
CREATE OR REPLACE FUNCTION nodes.getRemoteHighs() RETURNS TABLE(  _clockid bigint, _tsn bigint ) AS $$
   BEGIN
     RETURN QUERY
        select clockid, tsn from nodes.highwatermarks;
   END;
$$ LANGUAGE plpgsql;

/* 
 * Read the OPLOG
 *
 * Read all entries for all systems (clockid = 0) or for specified system
 * ordered by time (tsn)
 */
CREATE OR REPLACE FUNCTION nodes.getOplogPerSystem( in_clockid bigint ) RETURNS TABLE(  _table_name text, _clockid bigint, _tsn bigint, _op text ) AS $$
   BEGIN
     IF in_clockid = 0 THEN
       RETURN QUERY
         SELECT table_name, clockid, tsn, op from nodes.oplog
          ORDER BY clockid, tsn DESC;
     ELSE
       RETURN QUERY
         SELECT table_name, clockid, tsn, op from nodes.oplog
          WHERE clockid = in_clockid
          ORDER BY tsn DESC;
     END IF;
   END;
$$ LANGUAGE plpgsql;

/* 
 * Read the OPLOG tail
 *
 * Read entries higher with respect to time (tsn) for all systems (clockid = 0) or for specified system
 * ordered by time (tsn)
 */
CREATE OR REPLACE FUNCTION nodes.getOplogTail( in_clockid bigint, in_tsn bigint ) RETURNS TABLE(  _table_name text, _clockid bigint, _tsn bigint, _op text ) AS $$
   BEGIN
     IF in_clockid = 0 THEN
       RETURN QUERY
          SELECT table_name, clockid, tsn, op from nodes.oplog
           WHERE tsn > in_tsn
           ORDER BY tsn  DESC;
     ELSE
       RETURN QUERY
          SELECT table_name, clockid, tsn, op from nodes.oplog
           WHERE clockid = in_clockid and tsn > in_tsn
           ORDER BY tsn  DESC;
     END IF;
   END;
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

/* General Anti-Entropy functions : to be used for synchronization only */
/* GET */
CREATE OR REPLACE FUNCTION nodes.ae_get_systems( _clockid bigint, _tsn bigint ) RETURNS  SETOF nodes.base  AS $$
   BEGIN
      RETURN QUERY 
        SELECT ckey, cval, url, data, clockid, tsn  from nodes.systems 
           where clockid = _clockid and tsn = _tsn;
   END;
$$ LANGUAGE plpgsql;

/* PUT */
CREATE OR REPLACE FUNCTION nodes.ae_put_systems( _ckey bytea, _cval bytea, _url text, _data json, _clockid bigint, _tsn bigint ) RETURNS VOID AS $$
   BEGIN
      LOOP
        BEGIN
         INSERT INTO nodes.systems( ckey, cval, url, data, clockid, tsn ) 
         VALUES (_ckey, _cval, _url, _data, _clockid, _tsn );
         EXCEPTION WHEN unique_violation THEN
           /*remove older versions, can there be more ?*/
           DELETE FROM nodes.systems where url = _url AND clockid = _clockid and tsn < _tsn;
        END;
      END LOOP;
   END;
$$ LANGUAGE plpgsql;

/* DELETE */
CREATE OR REPLACE FUNCTION nodes.ae_delete_systems( _clockid bigint, _tsn bigint ) RETURNS VOID AS $$
   BEGIN
        BEGIN
         DELETE FROM nodes.systems WHERE clockid = _clockid and tsn = _tsn; 
        END;
   END;
$$ LANGUAGE plpgsql;



/*
 * Sync functions for nodes.systems
 *
CREATE OR REPLACE FUNCTION nodes.SyncGet_Systems( in_clockid bigint, in_tsn bigint) RETURNS nodes.systems AS $$
   BEGIN
     RETURN QUERY
        select * from nodes.systems;
   END
$$ LANGUAGE plpgsql;
 */

