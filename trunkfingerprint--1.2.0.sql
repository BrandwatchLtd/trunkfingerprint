-- Calculates a fingerprint of the database structure

\echo Use "CREATE EXTENSION trunkfingerprint" to load this file. \quit

create type nsp_rel_att as (
       nsp name,
       rel name,
       col name
);

create type nsp_rel as (
       nsp name,
       rel name
);

--=================================================================================================

create or replace function _get_columns(
       p_table regclass,
       p_exclude_tables nsp_rel[],
       p_exclude_columns nsp_rel_att[]
) returns table (
       table_oid oid,
       column_list text,
       table_priority int
) as $f$
begin
       -- firstly, system catalogs
       return query
       select attrelid,
              string_agg(
                     case
                     -- do not show certain columns, as they contain non-universal ids or are unreliable in some kind
                     when (relname, attname) in (
                            ('pg_authid', 'rolpassword'), -- in future postgres version it may vary because of salted encryption
                            ('pg_database', 'datname'), -- we do not track db names
                            ('pg_database', 'datlastsysoid'), -- implementation detail
                            ('pg_database', 'datfrozenxid'), -- altered by maintenance processes
                            ('pg_database', 'datminmxid'), -- altered by maintenance processes
                            ('pg_database', 'dattablespace'), -- related to physical storage, not logical structure
                            ('pg_depend', 'objsubid'),    -- tracked in objid
                            ('pg_depend', 'refobjsubid'), -- tracked in refobjid
                            ('pg_description', 'objsubid'), -- tracked in objoid
                            ('pg_class', 'reltoastrelid'),
                            ('pg_class', 'relfrozenxid'),
                            ('pg_class', 'relminmxid'),
                            ('pg_class', 'relnatts'),
                            ('pg_class', 'reltuples'),
                            ('pg_class', 'relpages'),
                            ('pg_class', 'relallvisible'),
                            ('pg_attribute', 'attnum'),  -- we don't check attributes order
                            ('pg_attribute', 'attndims'),-- this is not enforced by postgres; CREATE TABLE LIKE does not copy it
                            ('pg_attrdef', 'adsrc'),     -- shows old values
                            ('pg_constraint', 'consrc'), -- shows old values
                            ('pg_class', 'relhasindex'),    -- shows old values
                            ('pg_class', 'relhaspkey'),     -- shows old values
                            ('pg_class', 'relhasrules'),    -- shows old values
                            ('pg_class', 'relhastriggers'), -- shows old values
                            ('pg_class', 'relhassubclass'),  -- shows old values
			    ('pg_class', 'relrewrite'), -- internal detail, inaccessible in PG11 onwards
			    ('pg_constraint', 'conparentid'),
			    ('pg_partitioned_table', 'partdefid'),
			    ('pg_proc', 'prosqlbody'),
                            ('pg_index', 'indcheckxmin')  -- is implementation-dependent, may be different depending on whether the index was created with CONCURRENTLY keyword
                     )
                     then 'null::int'
                     -- show object names instead of oids
                     when atttypid = 'oid'::regtype then
                            case when attname like '%namespace' then '(select nspname from pg_namespace where oid = foo.' || attname || ')'
                                 when attname like '%owner'
                                   or attname like '%grantor'
                                   or attname like '%member'
                                   or attname like '%datdba'
                                   or attname like '%role'
                                   or attname like '%roleid'
                                   or attname like '%user'      then '(select rolname from pg_roles where oid = foo.' || attname || ')'
                                 when attname like '%collation' then '(select collname from pg_collation where oid = foo.' || attname || ')'
                                 when attname like '%dbid'
                                   or attname like '%database'  then 'foo.' || attname || ' = 0' -- we do not track db names and anyway reject objects from other DBs
                                 when attname like '%server'    then '(select srvname from pg_foreign_server where oid = foo.' || attname || ')'
                                 when attname like '%fdw'       then '(select fdwname from pg_foreign_data_wrapper where oid = foo.' || attname || ')'
                                 when attname like '%lang'      then '(select lanname from pg_language where oid = foo.' || attname || ')'
                                 when attname like '%constraint'then '(select conname from pg_constraint where oid = foo.' || attname || ')'
                                 when attname like '%am'
                                   or attname like '%method'    then '(select amname from pg_am where oid = foo.' || attname || ')'
                                 when attname like '%family'    then '(select opfname from pg_opfamily where oid = foo.' || attname || ')'
                                 when attname like '%opr'
                                   or attname like '%op'
                                   or attname = any('{oprcom,oprnegate}')
                                                                then '(select oprname from pg_operator where oid = foo.' || attname || ')'
                                 when attname like '%opc'       then '(select opcname from pg_opclass where oid = foo.' || attname || ')'
                                 when attname like '%tablespace'then '(select spcname from pg_tablespace where oid = foo.' || attname || ')'
                                 when attname like '%relid'
                                   or attname like '%indid'
                                   or attname like '%classid'
                                   or attname like '%classoid'
                                   or attname like '%@_class' escape '@'
                                   or attname = any('{inhparent}')
                                   or (relname, attname) = ('pg_class', 'oid')
                                                                then attname || '::regclass::text'
                                 when attname like '%typid'
                                   or attname like '%type'
                                   or attname = any('{typelem,typarray,typbasetype,provariadic,oprleft,oprright,oprresult,
                                                      castsource,casttarget}')
                                   or (relname, attname) = ('pg_type', 'oid')
                                                                then attname || '::regtype::text'
                                 when attname like '%foid'
                                   or attname like '%func'
                                   or attname like '%validator'
                                   or attname = any('{laninline,fdwhandler}')
                                   or (relname, attname) = ('pg_proc', 'oid')
                                                                then attname || '::regprocedure::text'
                                 when (relname, attname) = ('pg_class', 'relfilenode')
                                                                then 'null::int'
                                 when attname = 'oid' -- ignore those as the rows are uniquely identified by some other column
                                  and relname = any('{pg_authid,pg_attrdef,pg_user_mapping,pg_constraint,pg_language,pg_rewrite,pg_extension,
                                                      pg_foreign_data_wrapper,pg_foreign_server,pg_foreign_table,pg_policy,pg_default_acl,
                                                      pg_trigger,pg_event_trigger,pg_cast,pg_enum,pg_namespace,pg_conversion,pg_transform,
                                                      pg_operator,pg_opfamily,pg_opclass,pg_am,pg_amop,pg_amproc,pg_collation,pg_database}'::text[])
                                                                then 'null::int'
                                 when
                                          (relname in ('pg_depend', 'pg_shdepend') and attname = any('{refobjid,objid}'))
                                          or
                                          (relname in ('pg_init_privs', 'pg_description', 'pg_shdescription') and attname = 'objoid')
                                                                then format($$
                                   case %1$s::regclass::text
                                   when 'pg_am' then (
                                          select amname::text
                                          from pg_am
                                          where oid = %2$s
                                         )
                                   when 'pg_amop' then (
                                          select (amname, opfname, nspname, amoplefttype::regtype, amoprighttype::regtype, amoppurpose)::text
                                          from pg_amop
                                          join pg_opfamily on pg_opfamily.oid = amopfamily
                                          join pg_am on pg_am.oid = opfmethod
                                          join pg_namespace on pg_namespace.oid = opfnamespace
                                          where pg_amop.oid = %2$s
                                   )
                                   when 'pg_amproc' then (
                                          select (amname, opfname, nspname, amproclefttype::regtype, amprocrighttype::regtype, amprocnum)::text
                                          from pg_amproc
                                          join pg_opfamily on pg_opfamily.oid = amprocfamily
                                          join pg_am on pg_am.oid = opfmethod
                                          join pg_namespace on pg_namespace.oid = opfnamespace
                                          where pg_amproc.oid = %2$s
                                   )
                                   when 'pg_attrdef' then (
                                          select (attrelid::regclass::text, attname)::text
                                          from pg_attribute
                                          join pg_attrdef on attnum = adnum
                                                         and attrelid = adrelid
                                          where pg_attrdef.oid = %2$s
                                         )
                                   when 'pg_authid' then (
                                          select rolname::text
                                          from pg_authid
                                          where oid = %2$s
                                         )
                                   when 'pg_cast' then (
                                          select (castsource::regtype::text, casttarget::regtype::text)::text
                                          from pg_cast
                                          where oid = %2$s
                                         )
                                   when 'pg_class' then
                                         case %3$s
                                         when 0 then %2$s::regclass::text
                                         else (
                                          select (%2$s::regclass, attname)::text
                                          from pg_attribute
                                          where attrelid = %2$s
                                            and attnum = %3$s
                                         )
                                         end
                                   when 'pg_collation' then (
                                          select (nspname, collname, collencoding)::text
                                          from pg_collation
                                          join pg_namespace on pg_namespace.oid = collnamespace
                                          where pg_collation.oid = %2$s
                                         )
                                   when 'pg_constraint' then (
                                          select (conrelid::regclass::text, conname)::text -- is it unique?
                                          from pg_constraint
                                          where oid = %2$s
                                         )
                                   when 'pg_conversion' then (
                                          select (nspname, conname)::text
                                          from pg_conversion
                                          join pg_namespace on pg_namespace.oid = connamespace
                                          where pg_conversion.oid = %2$s
                                         )
                                   when 'pg_database' then (
                                          select datname::text
                                          from pg_database
                                          where oid = %2$s
                                         )
                                   when 'pg_default_acl' then (
                                          select (rolname, nspname, defaclobjtype)::text
                                          from pg_default_acl
                                          join pg_namespace on pg_namespace.oid = defaclnamespace
                                          join pg_roles on pg_roles.oid = defaclrole
                                          where pg_default_acl.oid = %2$s
                                         )
                                   when 'pg_event_trigger' then (
                                          select evtname::text
                                          from pg_event_trigger
                                          where oid = %2$s
                                         )
                                   when 'pg_extension' then (
                                          select extname::text
                                          from pg_extension
                                          where oid = %2$s
                                         )
                                   when 'pg_foreign_data_wrapper' then (
                                          select fdwname::text
                                          from pg_foreign_data_wrapper
                                          where oid = %2$s
                                         )
                                   when 'pg_foreign_server' then (
                                          select srvname::text
                                          from pg_foreign_server
                                          where oid = %2$s
                                         )
                                   when 'pg_language' then (
                                          select lanname::text
                                          from pg_language
                                          where oid = %2$s
                                         )
                                   when 'pg_namespace' then (
                                          select nspname::text
                                          from pg_namespace
                                          where oid = %2$s
                                         )
                                   when 'pg_opclass' then (
                                          select (amname, opcname, nspname)::text
                                          from pg_opclass
                                          join pg_am on pg_am.oid = opcmethod
                                          join pg_namespace on pg_namespace.oid = opcnamespace
                                          where pg_opclass.oid = %2$s
                                         )
                                   when 'pg_opfamily' then (
                                          select (amname, opfname, nspname)::text
                                          from pg_opfamily
                                          join pg_am on pg_am.oid = opfmethod
                                          join pg_namespace on pg_namespace.oid = opfnamespace
                                          where pg_opfamily.oid = %2$s
                                         )
                                   when 'pg_operator' then (
                                          select (oprname, oprleft::regtype, oprright::regtype, nspname)::text
                                          from pg_operator
                                          join pg_namespace on pg_namespace.oid = oprnamespace
                                          where pg_operator.oid = %2$s
                                         )
                                   when 'pg_proc' then %2$s::regprocedure::text
                                   when 'pg_rewrite' then (
                                          select (ev_class::regclass, rulename)::text
                                          from pg_rewrite
                                          where oid = %2$s
                                         )
                                   when 'pg_tablespace' then (
                                          select spcname::text
                                          from pg_tablespace
                                          where oid = %2$s
                                         )
                                   when 'pg_trigger' then (
                                          select (tgrelid::regclass, tgname)::text
                                          from pg_trigger
                                          where oid = %2$s
                                         )
                                   when 'pg_ts_config' then %2$s::regconfig::text
                                   when 'pg_ts_dict' then %2$s::regdictionary::text
                                   when 'pg_ts_parser' then (
                                          select (nspname, prsname)::text
                                          from pg_ts_parser
                                          join pg_namespace on pg_namespace.oid = prsnamespace
                                          where pg_ts_parser.oid = %2$s
                                         )
                                   when 'pg_ts_template' then (
                                          select (nspname, tmplname)::text
                                          from pg_ts_template
                                          join pg_namespace on pg_namespace.oid = tmplnamespace
                                          where pg_ts_template.oid = %2$s
                                         )
                                   when 'pg_type' then %2$s::regtype::text
                                   when 'pg_user_mapping' then (
                                          select (rolname, srvname)::text
                                          from pg_user_mapping
                                          join pg_roles on pg_roles.oid = umuser
                                          join pg_foreign_server on pg_foreign_server.oid = umserver
                                          where pg_user_mapping.oid = %2$s
                                         )
                                   when '-' then '-'::text
                                   else _error('pg_depend tracking for the type not implemented: ' || %1$s::regclass::text)
                                   end
                                                                    $$,
                                                                    case attname
                                                                      when 'objoid' then 'classoid'
                                                                      when 'objid' then 'classid'
                                                                      when 'refobjid' then 'refclassid'
                                                                    end /*%1$s*/,
                                                                    attname /*%2$s*/,
                                                                    case when relname in ('pg_depend', 'pg_description')
                                                                         then case attname
                                                                                when 'objoid' then 'objsubid'
                                                                                when 'objid' then 'objsubid'
                                                                                when 'refobjid' then 'refobjsubid'
                                                                              end /*%3$s*/
                                                                         else '0::oid' -- no subids in pg_shdepend or pg_shdescription
                                                                    end
                                                                )
                                 else _error('attempt to return bare oid: ' || relname || '.' || attname)
                            end
                     -- show object names arrays instead of oidvectors
                     when atttypid = any('{oidvector,oid[]}'::regtype[]) then
                            case when attname like '%collation'            then '(select array_agg(collname order by ord)
                                                                                 from unnest(' || attname || ') with ordinality as foo(colloid,ord)
                                                                                 join pg_collation on pg_collation.oid = colloid)'
                                 when attname in ('indclass', 'partclass') then '(select array_agg(opcname order by ord)
                                                                                 from unnest(' || attname || ') with ordinality as foo(opcoid,ord)
                                                                                 join pg_opclass on pg_opclass.oid = opcoid)'
                                 when attname like '%op'                   then '(select array_agg(oprname order by ord)
                                                                                 from unnest(' || attname || ') with ordinality as foo(oproid,ord)
                                                                                 join pg_operator on pg_operator.oid = oproid)'
                                 when attname like '%roles'
                                   or attname = 'pg_group'                 then '(select array_agg(rolname order by ord)
                                                                                 from unnest(' || attname || ') with ordinality as foo(roloid,ord)
                                                                                 join pg_roles on pg_roles.oid = roloid)'
                                 when attname like '%types'                then attname || '::regtype[]::text'
                                 when attname = 'extconfig'                then attname || '::regclass[]::text'
                                 else _error('attempt to return bare oidvector: ' || relname || '.' || attname)
                            end
                     -- show something pretty-formatted instead of pg_node_trees
                     when atttypid = 'pg_node_tree'::regtype then
                            case relname || '.' || attname
                                 when 'pg_attrdef.adbin'               then 'pg_get_expr(adbin, adrelid, true)'
                                 when 'pg_constraint.conbin'           then 'pg_get_constraintdef(oid, true)'
                                 when 'pg_class.relpartbound'          then 'pg_catalog.pg_get_expr(relpartbound, oid)'
                                 when 'pg_index.indexprs'              then 'pg_get_expr(indexprs, indrelid, true)'
                                 when 'pg_index.indpred'               then 'pg_get_expr(indpred, indrelid, true)'
                                 when 'pg_partitioned_table.partexprs' then 'pg_get_partkeydef(partrelid)'
                                 when 'pg_proc.proargdefaults'         then 'pg_catalog.pg_get_function_arguments(oid)'
                                 when 'pg_rewrite.ev_action'           then 'pg_catalog.pg_get_ruledef(oid, true)' -- NB: this function call takes most of the time
                                 when 'pg_rewrite.ev_qual'             then 'null::int' -- already tracked one line above
                                 when 'pg_trigger.tgqual'              then 'pg_get_triggerdef(oid, true)'
                                 when 'pg_type.typdefaultbin'          then 'null::int' -- already tracked in pg_type.typedefault
                                 else _error('attempt to return bare pg_node_tree: ' || relname || '.' || attname)
                            end
                     -- get certain arrays sorted
                     when atttypid = 'aclitem[]'::regtype
                       or (atttypid = 'text[]'::regtype and (attname like '%options' or attname like '%config'))
                       or (relname, attname) = ('pg_event_trigger', 'evttags')
                     then format('(select array_agg(item::text order by item::text) from unnest(%I) item)', attname)
                     -- show column names instead of attnums
                     when (relname, attname) = ('pg_attrdef', 'adnum')
                     then '(select attname from pg_attribute where attnum = adnum and attrelid = adrelid)'
                     when (relname, attname) = ('pg_index', 'indkey')
                     then _get_attribute_names_code('indrelid', 'indkey')
                     when (relname, attname) = ('pg_partitioned_table', 'partattrs')
                     then _get_attribute_names_code('partrelid', 'partattrs')
                     when (relname, attname) = ('pg_trigger', 'tgattr')
                     then _get_attribute_names_code('tgrelid', 'tgattr')
                     -- for check constraints ignore column order
                     when (relname, attname) = ('pg_constraint', 'conkey')
                     then _get_attribute_names_code('conrelid', 'conkey', 'contype <> ''c''')
                     when (relname, attname) = ('pg_constraint', 'confkey')
                     then _get_attribute_names_code('confrelid', 'confkey')
                     -- show all the rest as is
                     else attname::text end,
                     ', '
                     order by attnum
              ),
              case
                     when relname in ('pg_foreign_data_wrapper', 'pg_language') then 100
                     when relname in ('pg_foreign_server', 'pg_namespace') then 200
                     when relname in ('pg_user_mapping') then 220
                     -- 250 is default
                     when relname in ('pg_attribute', 'pg_index', 'pg_inherits',
                                      'pg_auth_members', 'pg_db_role_setting',
                                      'pg_conversion') then 300
                     when relname in ('pg_attrdef', 'pg_constraint') then 330
                     when relname in ('pg_trigger') then 360
                     when relname in ('pg_description', 'pg_shdescription') then 400
                     when relname in ('pg_depend', 'pg_shdepend') then 450
                     --
                     else 250
              end
       from pg_attribute
       join pg_class on pg_class.oid = pg_attribute.attrelid
       join pg_namespace on pg_namespace.oid = relnamespace
       where nspname = 'pg_catalog'
         and relkind = 'r'
         and relname <> all('{pg_statistic,pg_statistic_ext_data,pg_largeobject,pg_largeobject_metadata}'::name[] || -- those are rather data than structure
                            '{pg_tablespace}'::name[] || -- totally physical, does not relate to logical DB structure
                            '{pg_seclabel,pg_shseclabel}'::name[] -- TBD
                            )
         and relname not like 'pg@_ts@_%' escape '@' -- TBD: FTS-related
         and (attnum > 0 or attname = 'oid')
         and (p_table = pg_class.oid) is not false
         and not attisdropped -- I don't think there could be dropped columns in system catalogs but just in case
       group by attrelid,
                relname
       order by relname;

       -- then data tables and sequences
       return query
       select pg_attribute.attrelid,
              string_agg(
                     case
                     -- ignore explicitly excluded columns
                     when (nspname, relname, attname) = any(p_exclude_columns)
                       -- ignore columns with unstable defaults
                       or pg_get_expr(adbin, adrelid) ilike 'nextval(%)' or pg_get_expr(adbin, adrelid) ilike '%now()%'
                       or upper(pg_get_expr(adbin, adrelid)) = 'CURRENT_TIMESTAMP'
                       or pg_get_expr(adbin, adrelid) ilike '%gen\_random\_bytes(%)%'
                       -- ignore referring to those as well (TODO: recursive?)
                       or (attrelid, attnum) in (
                            select conrelid,
                                   conattnum
                            from pg_constraint
                            cross join lateral unnest(conkey, confkey) fk (conattnum, refattnum)
                            join pg_attrdef refattrdef on adrelid = confrelid
                                                      and adnum = refattnum
                            where contype = 'f'
                              and (pg_get_expr(adbin, adrelid) like 'nextval(%)' or pg_get_expr(adbin, adrelid) like '%now()%')
                          )
                     then 'null::int'
                     else attname
                     end,
                     ', '
                     order by attname
              ),
              500
       from pg_attribute
       join pg_class on pg_class.oid = pg_attribute.attrelid
       join pg_namespace on pg_namespace.oid = pg_class.relnamespace
       left join pg_attrdef on adrelid = attrelid
                           and adnum = attnum
       where attnum > 0
         and not attisdropped
         and relkind in ('r', 'S')
         and (p_table = pg_class.oid) is not false
         and (nspname, relname) <> all(p_exclude_tables)
         and nspname not like 'pg\_%'
         and nspname <> 'information_schema'
         and ('pg_class'::regclass, pg_class.oid) not in (table excluded_objects)
       group by 1;
end;
$f$ language plpgsql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _get_columns(regclass, nsp_rel[], nsp_rel_att[])
       is 'Provides a list of expressions to select from it for each system catalog.';

--=================================================================================================

create or replace function _get_excluded_objects(p_exclude_schemas name[]) returns table (
       classid oid,
       objid oid
) as $f$
begin
       return query execute
       $sql$
              with
              excluded_namespace as (
                     select tableoid classid, oid objid
                     from pg_namespace
                     where nspname = any($1)
                        or nspname like 'pg\_temp\_%'
                        or nspname like 'pg\_toast\_temp\_%'
              ),
              excluded_proc as (
                     select tableoid classid, oid objid
                     from pg_proc
                     where pronamespace in (select objid from excluded_namespace)
              ),
              excluded_opfamily as (
                     select tableoid classid, oid objid
                     from pg_opfamily
                     where opfnamespace in (select objid from excluded_namespace)
              ),
              excluded_class as (
                     select tableoid classid, oid objid
                     from pg_class
                     where relnamespace in (select objid from excluded_namespace)
              ),
              excluded_ts_config as (
                     select tableoid classid, oid objid
                     from pg_ts_config
                     where cfgnamespace in (select objid from excluded_namespace)
              ),
              excluded_type as (
                     select tableoid classid, oid objid
                     from pg_type
                     where typnamespace in (select objid from excluded_namespace)
              )
              table excluded_namespace
           union all
              table excluded_proc
           union all
              table excluded_opfamily
           union all
              table excluded_class
           union all
              table excluded_ts_config
           union all
              table excluded_type
           union all
              select tableoid classid, oid objid
              from pg_conversion
              where connamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_opclass
              where opcnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_operator
              where oprnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_collation
              where collnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_default_acl
              where defaclnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_extension
              where extnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_ts_dict
              where dictnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_ts_parser
              where prsnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_ts_template
              where tmplnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_cast
              where castfunc in (select objid from excluded_proc)
           union all
              select tableoid classid, oid objid
              from pg_amop
              where amopfamily in (select objid from excluded_opfamily)
           union all
              select tableoid classid, oid objid
              from pg_amproc
              where amprocfamily in (select objid from excluded_opfamily)
           union all
              select tableoid classid, oid objid
              from pg_rewrite
              where ev_class in (select objid from excluded_class)
           union all
              select tableoid classid, oid objid
              from pg_constraint
              where conrelid in (select objid from excluded_class)
           union all
              select tableoid classid, oid objid
              from pg_trigger
              where tgrelid in (select objid from excluded_class)
           union all
              select tableoid classid, oid objid
              from pg_attrdef
              where adrelid in (select objid from excluded_class)
           union all
              select tableoid classid, oid objid
              from pg_enum
              where enumtypid in (select objid from excluded_type)
       $sql$
       ||
       _if_version_at_least(95000, $sql$
           union all
              select tableoid classid, oid objid
              from pg_transform
              where trftype in (select objid from excluded_type)
           union all
              select tableoid classid, oid objid
              from pg_policy
              where polrelid in (select objid from excluded_class)
       $sql$)
       ||
       _if_version_at_least(100000, $sql$
           union all
              select tableoid classid, oid objid
              from pg_statistic_ext
              where stxnamespace in (select objid from excluded_namespace)
           union all
              select tableoid classid, oid objid
              from pg_publication_rel
              where prrelid in (select objid from excluded_class);
       $sql$)
       using p_exclude_schemas;
end;
$f$ language plpgsql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _get_excluded_objects(name[])
       is 'Given a list of schemas to exclude, this function retuns all the objects kind of in these schemas';

--=================================================================================================

create or replace function _get_not_restricted_condition_for_catalog(p_oid_column text, p_catalog_oid regclass)
       returns text
as $f$
       select $$foo.$$ || p_oid_column || $$ not in (
                   select e.objid
                   from excluded_objects e
                   where e.classid = $$ || p_catalog_oid::oid || $$
              ) $$;
$f$ language sql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _get_not_restricted_condition_for_catalog(text, regclass) 
       is 'Helper function for building SQL';

--=================================================================================================

create or replace function _get_single_table_sql(p_level int, p_table_oid oid, p_column_list text)
       returns text as $f$
declare
       c_not_in_all_excluded_objects_sql text := $$
              not in (
                     select e.classid,
                            e.objid
                     from excluded_objects e
              )
       $$;
       c_notoast text := $$
              not in (
                     select pg_class.oid
                     from pg_class
                     join pg_namespace on pg_namespace.oid = relnamespace
                     where nspname = 'pg_toast'
              )$$;
       l_is_catalog bool;
       l_is_shared bool;
       -- optionally schema-qualified name, according to search path
       l_table_oqname text := p_table_oid::regclass::text;
begin
       select nspname = 'pg_catalog',
              relisshared
       into l_is_catalog,
            l_is_shared
       from pg_class
       join pg_namespace on pg_namespace.oid = pg_class.relnamespace
       where pg_class.oid = p_table_oid;

       return
       -- select list
       case when p_level = 2 then $$
              select ($$ || p_column_list || $$)
       $$ else $$
              select hash_array(coalesce(
                            array_agg(
                                   hashtext(($$ || p_column_list || $$)::text)
                                   order by
                                   hashtext(($$ || p_column_list || $$)::text)
                            ),
                            '{}'
                     ))::text
       $$ end || $$
       -- from
       from only $$ || p_table_oid::regclass || $$ foo
       -- conditions
       $$ || (
              select coalesce('where ' || string_agg(item, ' and ') filter (where item is not null), '')
              from unnest(array[
                     -- condition for not being in restricted schema
                     case
                            -- all-objects catalogs
                            when l_table_oqname in ('pg_depend', 'pg_shdepend')
                                then $$ (classid, objid) $$ || c_not_in_all_excluded_objects_sql
                            when l_table_oqname in ('pg_init_privs',
                                                                'pg_description', 'pg_shdescription',
                                                                'pg_seclabel', 'pg_shseclabel')
                                then $$ (classoid, objoid) $$ || c_not_in_all_excluded_objects_sql
                            -- catalogs without oid column (how on the earth they exist!)
                            when l_table_oqname = 'pg_aggregate'
                                then _get_not_restricted_condition_for_catalog('aggfnoid', 'pg_proc')
                            when l_table_oqname = 'pg_attribute'
                                then _get_not_restricted_condition_for_catalog('attrelid', 'pg_class')
                            when l_table_oqname = 'pg_auth_members'
                                then _get_not_restricted_condition_for_catalog('member', 'pg_authid')
                                     || ' and ' ||
                                     _get_not_restricted_condition_for_catalog('grantor', 'pg_authid')
                            when l_table_oqname = 'pg_db_role_setting'
                                then _get_not_restricted_condition_for_catalog('setdatabase', 'pg_database')
                                     || ' and ' ||
                                     _get_not_restricted_condition_for_catalog('setrole', 'pg_authid')
                            when l_table_oqname = 'pg_index'
                                then _get_not_restricted_condition_for_catalog('indexrelid', 'pg_class')
                            when l_table_oqname = 'pg_init_privs'
                                then _get_not_restricted_condition_for_catalog('indexrelid', 'pg_class')
                            when l_table_oqname = 'pg_foreign_table'
                                then _get_not_restricted_condition_for_catalog('ftrelid', 'pg_class')
                            when l_table_oqname = 'pg_inherits'
                                then _get_not_restricted_condition_for_catalog('inhrelid', 'pg_class')
                            when l_table_oqname = 'pg_partitioned_table'
                                then _get_not_restricted_condition_for_catalog('partrelid', 'pg_class')
                            when l_table_oqname = 'pg_sequence'
                                then _get_not_restricted_condition_for_catalog('seqrelid', 'pg_class')
                            when l_table_oqname = 'pg_subscription_rel'
                                then _get_not_restricted_condition_for_catalog('srrelid', 'pg_class')
                            when l_table_oqname = 'pg_range'
                                then _get_not_restricted_condition_for_catalog('rngtypid', 'pg_type')
                            when l_table_oqname = 'pg_ts_config_map'
                                then _get_not_restricted_condition_for_catalog('mapcfg', 'pg_ts_config')
                            -- these guys has no numeric IDs at all
                            when l_table_oqname in ('pg_replication_origin', 'pg_pltemplate')
                                then 'true'
                            -- ordinary catalogs
                            when l_is_catalog
                                then _get_not_restricted_condition_for_catalog('oid', p_table_oid)
                     end,
                     -- check only current DB in global catalogs with db-related objects
                     case when l_is_catalog then
                           case l_table_oqname
                               when 'pg_database' then 'oid'
                               when 'pg_db_role_setting' then 'setdatabase'
                               when 'pg_shdepend' then 'dbid'
                           end || ' in (0, (select oid from pg_database where datname = current_database()))'
                     end,
                     -- other catalog-specific conditions
                     case l_table_oqname
                           when 'pg_namespace' -- exclude temp schemas
                               then $$ nspname not like 'pg@_temp@_%' escape '@'
                                   and nspname not like 'pg@_toast@_temp@_%' escape '@' $$
                           when 'pg_trigger' -- exclude internal trigger, as their names are autogenerated
                               then $$ not tgisinternal $$
                           when 'pg_attribute' -- exclude dropped columns and columns of toasted tables and indexes
                               then $$ not attisdropped
                                       and attrelid not in (
                                              select pg_class.oid
                                              from pg_class
                                              join pg_namespace on pg_namespace.oid = relnamespace
                                              where nspname = 'pg_toast'
                                                 or relkind = 'i'
                                             ) $$
                           when 'pg_attrdef' -- exclude dropped columns defaults
                               then $$ (adrelid, adnum) not in (select attrelid, attnum from pg_attribute where attisdropped) $$
                           when 'pg_class' -- exclude toast tables
                               then $$ not relisshared and oid $$ || c_notoast
                           when 'pg_index' -- exclude toast tables indices
                               then $$ indrelid $$ || c_notoast
                           when 'pg_type' -- exclude toast table types
                               then $$ typrelid $$ || c_notoast
                           when 'pg_depend' -- exclude rule-depends-on-column links
                               then $$ not (
                                           classid = 'pg_rewrite'::regclass and
                                           refclassid = 'pg_class'::regclass and
                                           refobjsubid <> 0
                                       ) and (
                                           classid <> 'pg_class'::regclass or objid $$ || c_notoast || $$
                                       ) and (
                                           refclassid <> 'pg_class'::regclass or refobjid $$ || c_notoast || $$
                                       ) and (
                                           deptype not in ('i', 'p')
                                       )$$
                     end
              ]) item
       ) ||
       -- ordering
       case
       when p_level = 2
       then $$ order by ($$ || p_column_list || $$)$$
       else $$ $$
       end;
end;
$f$ language plpgsql set search_path to pg_catalog, @extschema@, pg_temp;

comment on function _get_single_table_sql(int, oid, text)
       is 'Build an SQL for a single table';

--=================================================================================================

create or replace function __error(p_message text) returns text as $f$
begin
       raise '%', p_message;
end;
$f$ language plpgsql;
comment on function __error(text) is 'Report error when called';

--=================================================================================================

create or replace function _error(p_message text) returns text as $f$
       select format('__error(%L)', p_message);
$f$ language sql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _error(text) is 'Inject error reporting into a string building expression';

--=================================================================================================

create or replace function _if_version_at_least(p_version int, p_code text) returns text as $f$
       select case
                     when current_setting('server_version_num')::bigint >= p_version
                     then p_code
                     else ''
              end;
$f$ language sql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _if_version_at_least(int, text) is 'Shortcut for generating portable code';

--=================================================================================================

create or replace function _get_attribute_names_code(
       p_relid_expr text,
       p_attr_positions_expr text,
       p_preserve_order_condition text default 'true'
) returns text as $f$
       select format(
                     $$
                            array(
                                   select attname
                                   from unnest(%1$s::int2[]) with ordinality as _(att_num_from_vector, ord)
                                   join pg_attribute on attrelid = %2$s
                                                    and attnum = att_num_from_vector
                                   order by case when %3$s then ord end,
                                            case when not %3$s then attname end
                            )
                     $$,
                     p_attr_positions_expr,
                     p_relid_expr,
                     p_preserve_order_condition
              );
$f$ language sql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _get_attribute_names_code(text, text, text)
       is 'Generates code to convert attribute index list into a list of their names,
preserving the order if p_preserve_order_condition evaluates to true
and using alphabetical order otherwise';

--=================================================================================================

create or replace function get_db_fingerprint(
       p_level int default 0,
       p_table regclass default null,
       p_data bool default null,
       p_exclude_schemas name[] default '{}',
       p_exclude_tables_data nsp_rel[] default '{}',
       p_exclude_columns_data nsp_rel_att[] default '{}'
)
returns setof varchar as $f$
declare
       table_oid oid;
       column_list text;
       one_hash text;
       hashes text := '';
       single_table_sql text;
begin
       create temp table excluded_objects on commit drop as
              select * from _get_excluded_objects(p_exclude_schemas);

       for table_oid, column_list in
              select f.table_oid,
                     f.column_list
              from _get_columns(p_table, p_exclude_tables_data, p_exclude_columns_data) f
              order by f.table_priority,
                       f.table_oid::regclass::text collate "C"
       loop
              if p_table is null and p_level = 2 then
                     return next table_oid::regclass::text;
              end if;

              if p_level = 2 then
                     /* title line to show what the columns are about */
                     return next table_oid::regclass || ' ' || replace(column_list, chr(10), ' ');
              end if;

              single_table_sql := _get_single_table_sql(p_level, table_oid, column_list);

              raise debug '%: running %', clock_timestamp(), single_table_sql;

              for one_hash in execute single_table_sql loop
                     if p_level >= 1 then
                            return next table_oid::regclass || ' ' || one_hash;
                     else
                            hashes := hashes || one_hash;
                     end if;
              end loop;
       end loop;

       if p_level = 0 then
              return next md5(hashes);
       end if;

       drop table excluded_objects;
end;
$f$ language plpgsql set search_path to pg_catalog, @extschema@, pg_temp;

comment on function get_db_fingerprint(int, regclass, bool, name[], nsp_rel[], nsp_rel_att[])
       is 'Main function to call. All arguments are optional.
p_level                -- Could be 0 (single fingerprint, default), 1 (one value for each catalog) or 2 (a line per each catalog object).
p_table                -- Specify to restrict to a single catalog or table. All the tables and catalogs by default.
p_data                 -- Examine data only if true, structure only if false, both if null (default).
p_exclude_schemas      -- Exclude specified schemas from data and structure examinations.
p_exclude_tables_data  -- Exclude specified tables from data examination only.
p_exclude_columns_data -- Exclude specified table columns from data examination only.
';
