-- Database generated with pgModeler (PostgreSQL Database Modeler).
-- pgModeler  version: 0.9.3-beta1
-- PostgreSQL version: 13.0
-- Project Site: pgmodeler.io
-- Model Author: ---

-- Database creation must be performed outside a multi lined SQL file. 
-- These commands were put in this file only as a convenience.
-- 
-- object: new_database | type: DATABASE --
-- DROP DATABASE IF EXISTS new_database;
CREATE DATABASE new_database;
-- ddl-end --


-- object: public.video | type: TABLE --
-- DROP TABLE IF EXISTS public.video CASCADE;
CREATE TABLE public.video (
	id varchar NOT NULL,
	title_raw varchar NOT NULL,
	title_processed varchar NOT NULL,
	description text,
	published_at date NOT NULL,
	language varchar NOT NULL,
	duration time NOT NULL,
	view_count integer NOT NULL,
	like_count integer NOT NULL,
	thumbnails text NOT NULL,
	comment_count integer NOT NULL,
	topic text NOT NULL,
	id_channel varchar NOT NULL,
	CONSTRAINT video_pk PRIMARY KEY (id)

);
-- ddl-end --
ALTER TABLE public.video OWNER TO postgres;
-- ddl-end --

-- object: public.category | type: TABLE --
-- DROP TABLE IF EXISTS public.category CASCADE;
CREATE TABLE public.category (
	id varchar NOT NULL,
	name varchar NOT NULL,
	CONSTRAINT category_pk PRIMARY KEY (id)

);
-- ddl-end --
ALTER TABLE public.category OWNER TO postgres;
-- ddl-end --

-- object: public.video_category | type: TABLE --
-- DROP TABLE IF EXISTS public.video_category CASCADE;
CREATE TABLE public.video_category (
	id serial NOT NULL,
	id_video varchar NOT NULL,
	id_category varchar NOT NULL,
	CONSTRAINT video_category_pk PRIMARY KEY (id)

);
-- ddl-end --

-- object: video_fk | type: CONSTRAINT --
-- ALTER TABLE public.video_category DROP CONSTRAINT IF EXISTS video_fk CASCADE;
ALTER TABLE public.video_category ADD CONSTRAINT video_fk FOREIGN KEY (id_video)
REFERENCES public.video (id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: category_fk | type: CONSTRAINT --
-- ALTER TABLE public.video_category DROP CONSTRAINT IF EXISTS category_fk CASCADE;
ALTER TABLE public.video_category ADD CONSTRAINT category_fk FOREIGN KEY (id_category)
REFERENCES public.category (id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: public.comment | type: TABLE --
-- DROP TABLE IF EXISTS public.comment CASCADE;
CREATE TABLE public.comment (
	id varchar NOT NULL,
	text varchar NOT NULL,
	published_at date NOT NULL,
	is_response bool NOT NULL,
	like_count integer NOT NULL,
	is_possitive bool,
	is_formal bool,
	id_channel varchar NOT NULL,
	id_video varchar NOT NULL,
	CONSTRAINT comment_pk PRIMARY KEY (id)

);
-- ddl-end --
ALTER TABLE public.comment OWNER TO postgres;
-- ddl-end --

-- object: public.rating | type: TABLE --
-- DROP TABLE IF EXISTS public.rating CASCADE;
CREATE TABLE public.rating (
	id varchar NOT NULL,
	name varchar NOT NULL,
	CONSTRAINT rating_pk PRIMARY KEY (id)

);
-- ddl-end --
ALTER TABLE public.rating OWNER TO postgres;
-- ddl-end --

-- object: public.video_rating | type: TABLE --
-- DROP TABLE IF EXISTS public.video_rating CASCADE;
CREATE TABLE public.video_rating (
	id serial NOT NULL,
	id_video varchar NOT NULL,
	id_rating varchar NOT NULL,
	CONSTRAINT video_rating_pk PRIMARY KEY (id)

);
-- ddl-end --

-- object: video_fk | type: CONSTRAINT --
-- ALTER TABLE public.video_rating DROP CONSTRAINT IF EXISTS video_fk CASCADE;
ALTER TABLE public.video_rating ADD CONSTRAINT video_fk FOREIGN KEY (id_video)
REFERENCES public.video (id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: rating_fk | type: CONSTRAINT --
-- ALTER TABLE public.video_rating DROP CONSTRAINT IF EXISTS rating_fk CASCADE;
ALTER TABLE public.video_rating ADD CONSTRAINT rating_fk FOREIGN KEY (id_rating)
REFERENCES public.rating (id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: public.channel | type: TABLE --
-- DROP TABLE IF EXISTS public.channel CASCADE;
CREATE TABLE public.channel (
	id varchar NOT NULL,
	name varchar NOT NULL,
	language varchar NOT NULL,
	description text,
	suscriber_count integer NOT NULL,
	banner text NOT NULL,
	category_link text,
	CONSTRAINT channel_pk PRIMARY KEY (id)

);
-- ddl-end --
ALTER TABLE public.channel OWNER TO postgres;
-- ddl-end --

-- object: channel_fk | type: CONSTRAINT --
-- ALTER TABLE public.video DROP CONSTRAINT IF EXISTS channel_fk CASCADE;
ALTER TABLE public.video ADD CONSTRAINT channel_fk FOREIGN KEY (id_channel)
REFERENCES public.channel (id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --

-- object: video_published_at_index | type: INDEX --
-- DROP INDEX IF EXISTS public.video_published_at_index CASCADE;
CREATE INDEX video_published_at_index ON public.video
	USING btree
	(
	  published_at
	);
-- ddl-end --

-- object: video_language_index | type: INDEX --
-- DROP INDEX IF EXISTS public.video_language_index CASCADE;
CREATE INDEX video_language_index ON public.video
	USING btree
	(
	  language
	);
-- ddl-end --

-- object: channel_language_index | type: INDEX --
-- DROP INDEX IF EXISTS public.channel_language_index CASCADE;
CREATE INDEX channel_language_index ON public.channel
	USING btree
	(
	  language
	);
-- ddl-end --

-- object: comment_published_at_index | type: INDEX --
-- DROP INDEX IF EXISTS public.comment_published_at_index CASCADE;
CREATE INDEX comment_published_at_index ON public.comment
	USING btree
	(
	  published_at
	);
-- ddl-end --

-- object: video_fk | type: CONSTRAINT --
-- ALTER TABLE public.comment DROP CONSTRAINT IF EXISTS video_fk CASCADE;
ALTER TABLE public.comment ADD CONSTRAINT video_fk FOREIGN KEY (id_video)
REFERENCES public.video (id) MATCH FULL
ON DELETE RESTRICT ON UPDATE CASCADE;
-- ddl-end --


