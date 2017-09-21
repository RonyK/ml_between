


-- ---------------------------------------------------------------------
-- CREATE SEQUENCES
-- ---------------------------------------------------------------------
create sequence "ml_array_id_seq";


-- ---------------------------------------------------------------------
-- CREATE TABLES
-- ---------------------------------------------------------------------

--
--  Table: public.ml_array
--

create table "ml_array"
(
  ml_id bigint primary key default nextval('ml_array_id_seq'),
  name varchar,
  lowest integer,
  highest integer,
  scale_factor float,

  unique ( ml_id, name )
);

create table "ml_array_level"
(
	ml_array_id bigint references "ml_array" (ml_id) on delete cascade,
	level_array_id bigint references "array" (id) on delete cascade,
	level integer,
	cell_width float,

	unique(ml_array_id, level)
)

