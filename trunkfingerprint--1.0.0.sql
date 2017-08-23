-- this thing calculates a fingerprint of DB structure

create schema if not exists @extschema@;

--=================================================================================================

create or replace function _get_catalog_columns(p_catalog regclass) returns table (
       catalog_oid oid,
       column_list text
) as $f$
begin
       return query
       select attrelid,
              string_agg(
                     case
                     -- do not show certain columns, as they contain non-universal ids or are unreliable in some kind
                     when (relname, attname) in (
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
                            ('pg_attribute', 'attndims'),-- this is not enforced by postgres; create table like does not copy it
                            ('pg_attrdef', 'adsrc'),     -- shows old values
                            ('pg_constraint', 'consrc'), -- shows old values
                            ('pg_class', 'relhaspkey'),     -- shows old values
                            ('pg_class', 'relhasrules'),    -- shows old values
                            ('pg_class', 'relhastriggers'), -- shows old values
                            ('pg_class', 'relhassubclass')  -- shows old values
                     )
                     then 'null::int'
                     -- show object names instead of oids
                     when atttypid = 'oid'::regtype then
                            case when attname like '%namespace' then '(select nspname from pg_namespace where oid = foo.' || attname || ')'
                                 when attname like '%owner'
                                   or attname like '%role'
                                   or attname like '%user'      then '(select rolname from pg_roles where oid = foo.' || attname || ')'
                                 when attname like '%collation' then '(select collname from pg_collation where oid = foo.' || attname || ')'
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
                                 when attname = 'oid' -- ignore those as they are not used elsewhere
                                  and relname = any('{pg_attrdef,pg_user_mapping,pg_constraint,pg_language,pg_rewrite,pg_extension,
                                                      pg_foreign_data_wrapper,pg_foreign_server,pg_foreign_table,pg_policy,pg_default_acl,
                                                      pg_trigger,pg_event_trigger,pg_cast,pg_enum,pg_namespace,pg_conversion,pg_transform,
                                                      pg_operator,pg_opfamily,pg_opclass,pg_am,pg_amop,pg_amproc,pg_collation}'::text[])
                                                                then 'null::int'
                                 when
                                          (relname = 'pg_depend' and attname = any('{refobjid,objid}'))
                                          or
                                          (relname = 'pg_description' and attname = 'objoid')
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
                                   else 1/(1 - %2$s::int/%2$s::int)
                                          || '%% pg_depend tracking for this kind of objects not implemented %%'
                                   end
                                                                    $$,
                                                                    case attname
                                                                      when 'objoid' then 'classoid'
                                                                      when 'objid' then 'classid'
                                                                      when 'refobjid' then 'refclassid'
                                                                    end /*$1%s*/,
                                                                    attname /*$2%s*/,
                                                                    case attname
                                                                      when 'objoid' then 'objsubid'
                                                                      when 'objid' then 'objsubid'
                                                                      when 'refobjid' then 'refobjsubid'
                                                                    end /*$3%s*/
                                                                )
                                 else '% attempt to return bare oid % ' || attname
                            end
                     -- show object names arrays instead of oidvectors
                     when atttypid = any('{oidvector,oid[]}'::regtype[]) then
                            case when attname like '%collation' then '(select array_agg(collname order by ord)
                                                                      from unnest(' || attname || ') with ordinality as foo(colloid,ord)
                                                                      join pg_collation on pg_collation.oid = colloid)'
                                 when attname = 'indclass'      then '(select array_agg(opcname order by ord)
                                                                      from unnest(' || attname || ') with ordinality as foo(opcoid,ord)
                                                                      join pg_opclass on pg_opclass.oid = opcoid)'
                                 when attname like '%op'        then '(select array_agg(oprname order by ord)
                                                                      from unnest(' || attname || ') with ordinality as foo(oproid,ord)
                                                                      join pg_operator on pg_operator.oid = oproid)'
                                 when attname like '%roles'
                                   or attname = 'pg_group'      then '(select array_agg(rolname order by ord)
                                                                      from unnest(' || attname || ') with ordinality as foo(roloid,ord)
                                                                      join pg_roles on pg_roles.oid = roloid)'
                                 when attname like '%types'     then attname || '::regtype[]::text'
                                 when attname = 'extconfig'     then attname || '::regclass[]::text'
                                 else '% attempt to return bare oidvector % ' || attname
                            end
                     -- show something pretty-formatted instead of pg_node_trees
                     when atttypid = 'pg_node_tree'::regtype then
                            case relname || '.' || attname
                                 when 'pg_attrdef.adbin'       then 'pg_get_expr(adbin, adrelid, true)'
                                 when 'pg_constraint.conbin'   then 'pg_get_constraintdef(oid, true)'
                                 when 'pg_index.indexprs'      then 'pg_get_expr(indexprs, indrelid, true)'
                                 when 'pg_index.indpred'       then 'pg_get_expr(indpred, indrelid, true)'
                                 when 'pg_proc.proargdefaults' then 'pg_catalog.pg_get_function_arguments(oid)'
                                 when 'pg_rewrite.ev_action'   then 'pg_catalog.pg_get_ruledef(oid, true)' -- NB: this function call takes most of the time
                                 when 'pg_rewrite.ev_qual'     then 'null::int' -- already tracked one line above
                                 when 'pg_trigger.tgqual'      then 'pg_get_triggerdef(oid, true)'
                                 when 'pg_type.typdefaultbin'  then 'null::int' -- already tracked in pg_type.typedefault
                                 else '% attempt to return bare pg_node_tree % ' || attname
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
                     then '(select array_agg(attname order by ord) from unnest(indkey::int2[]) with ordinality as foo(attn,ord)
                            join pg_attribute on attnum = attn and attrelid = indrelid)'
                     when (relname, attname) = ('pg_trigger', 'tgattr')
                     then '(select array_agg(attname order by ord) from unnest(tgattr::int2[]) with ordinality as foo(attn,ord)
                            join pg_attribute on attnum = attn and attrelid = tgrelid)'
                     when (relname, attname) = ('pg_constraint', 'conkey')
                     then '(select array_agg(attname order by ord) from unnest(conkey) with ordinality as foo(attn,ord)
                            join pg_attribute on attnum = attn and attrelid = conrelid)'
                     when (relname, attname) = ('pg_constraint', 'confkey')
                     then '(select array_agg(attname order by ord) from unnest(confkey) with ordinality as foo(attn,ord)
                            join pg_attribute on attnum = attn and attrelid = confrelid)'
                     -- show all the rest as is
                     else attname::text end,
                     ', '
                     order by attnum
              )
       from pg_attribute
       join pg_class on pg_class.oid = pg_attribute.attrelid
       join pg_namespace on pg_namespace.oid = relnamespace
       where nspname = 'pg_catalog'
         and relkind = 'r'
         and relname <> all('{pg_statistic,pg_largeobject,pg_largeobject_metadata}'::name[] || -- those are rather data than structure
                            '{pg_seclabel}'::name[] -- TBD
                            )
         and relname not like 'pg@_ts@_%' escape '@' -- TBD: FTS-related
         and (attnum > 0 or attname = 'oid')
         and not relisshared
         and (p_catalog = attrelid) is not false
       group by attrelid,
                relname
       order by relname;
end;
$f$ language plpgsql set search_path to pg_catalog, @extschema@, pg_temp;
comment on function _get_catalog_columns(regclass)
       is 'Provides a list of expressions to select from it for each system catalog.';

--=================================================================================================

create or replace function _get_excluded_objects(p_exclude_schemas name[]) returns table (
       classid oid,
       objid oid
) as $f$
       with
       excluded_namespace as (
              select tableoid classid, oid objid
              from pg_namespace
              where nspname = any(p_exclude_schemas)
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
       where enumtypid in (select objid from excluded_type);
$f$ language sql set search_path to pg_catalog, @extschema@, pg_temp;
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

create or replace function _get_single_catalog_sql(p_level int, p_catalog_oid oid, p_column_list text)
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
begin
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
       from $$ || p_catalog_oid::regclass || $$ foo
       -- conditions:
       where $$ ||
       -- condition for not being in restricted schema
       case
              -- all-objects catalogs
              when p_catalog_oid = 'pg_depend'::regclass
                  then $$ (classid, objid) $$ || c_not_in_all_excluded_objects_sql
              when p_catalog_oid = any('{pg_description,pg_seclabel}'::regclass[])
                  then $$ (classoid, objoid) $$ || c_not_in_all_excluded_objects_sql
              -- catalogs without oid column (how on the earth they exist!)
              when p_catalog_oid = 'pg_aggregate'::regclass
                  then _get_not_restricted_condition_for_catalog('aggfnoid', 'pg_proc')
              when p_catalog_oid = 'pg_index'::regclass
                  then _get_not_restricted_condition_for_catalog('indexrelid', 'pg_class')
              when p_catalog_oid = 'pg_foreign_table'::regclass
                  then _get_not_restricted_condition_for_catalog('ftrelid', 'pg_class')
              when p_catalog_oid = 'pg_inherits'::regclass
                  then _get_not_restricted_condition_for_catalog('inhrelid', 'pg_class')
              when p_catalog_oid = 'pg_attribute'::regclass
                  then _get_not_restricted_condition_for_catalog('attrelid', 'pg_class')
              when p_catalog_oid = 'pg_ts_config_map'::regclass
                  then _get_not_restricted_condition_for_catalog('mapcfg', 'pg_ts_config')
              when p_catalog_oid = 'pg_range'::regclass
                  then _get_not_restricted_condition_for_catalog('rngtypid', 'pg_type')
              -- ordinary catalogs
              else
                  _get_not_restricted_condition_for_catalog('oid', p_catalog_oid)
       end ||
       -- other catalog-specific conditions
       case p_catalog_oid
             when 'pg_namespace'::regclass -- exclude temp schemas
                 then $$   and nspname not like 'pg@_temp@_%' escape '@'
                           and nspname not like 'pg@_toast@_temp@_%' escape '@' $$
             when 'pg_trigger'::regclass -- exclude internal trigger, as their names are autogenerated
                 then $$   and not tgisinternal $$
             when 'pg_attribute'::regclass -- exclude dropped columns and columns of toasted tables and indexes
                 then $$   and not attisdropped
                           and attrelid not in (
                                select pg_class.oid
                                from pg_class
                                join pg_namespace on pg_namespace.oid = relnamespace
                                where nspname = 'pg_toast'
                                   or relkind = 'i'
                               ) $$
             when 'pg_attrdef'::regclass -- exclude dropped columns defaults
                 then $$   and (adrelid, adnum)
                               not in (select attrelid, attnum from pg_attribute where attisdropped) $$
             when 'pg_class'::regclass -- exclude toast tables
                 then $$   and not relisshared
                           and oid $$ || c_notoast
             when 'pg_index'::regclass -- exclude toast tables indices
                 then $$   and indrelid $$ || c_notoast
             when 'pg_type'::regclass -- exclude toast table types
                 then $$   and typrelid $$ || c_notoast
             when 'pg_depend'::regclass -- exclude rule-depends-on-column links
                 then $$   and not (
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
             else $$ $$
       end ||
       -- ordering
       case
       when p_level = 2
       then $$ order by ($$ || p_column_list || $$)$$
       else $$ $$
       end;
end;
$f$ language plpgsql set search_path to pg_catalog, @extschema@, pg_temp;

comment on function _get_single_catalog_sql(int, oid, text)
       is 'Build an SQL for a single catalog';

--=================================================================================================

create or replace function get_db_structure_hash(
       p_level int default 0,
       p_catalog regclass default null,
       p_exclude_schemas name[] default '{}'
)
returns setof varchar as $f$
declare
       catalog_oid oid;
       column_list text;
       one_hash text;
       hashes text := '';
       single_catalog_sql text;
begin
       create temp table excluded_objects on commit drop as
              select * from _get_excluded_objects(p_exclude_schemas);

       for catalog_oid, column_list in select * from _get_catalog_columns(p_catalog) loop
              if p_catalog is null and p_level = 2 then
                     return next catalog_oid::regclass::text;
              end if;

              if p_level = 2 then
                     return next column_list;
              end if;

              single_catalog_sql := _get_single_catalog_sql(p_level, catalog_oid, column_list);

              raise debug '%: running %', clock_timestamp(), single_catalog_sql;

              for one_hash in execute single_catalog_sql loop
                     if p_level >= 1 then
                            return next catalog_oid::regclass || ' ' || one_hash;
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

comment on function get_db_structure_hash(int, regclass, name[])
       is 'Main function to call, p_level could be 0 (single fingerprint), '
          '1 (one value for each catalog) or 2 (a line per each catalog object)';
