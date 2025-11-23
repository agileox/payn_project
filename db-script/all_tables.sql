--
-- PostgreSQL database dump
--

\restrict ZfigvvY994YMTgviHJFqqhgVcALLu6cwVMLawaayq6oAmooiXtDoeY12Q7xRPkN

-- Dumped from database version 18.1
-- Dumped by pg_dump version 18.1

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cleaned_transactions; Type: TABLE; Schema: payn_etl; Owner: payn_user
--

CREATE TABLE payn_etl.cleaned_transactions (
    id integer NOT NULL,
    unnamed_id character varying(50),
    amt numeric,
    category character varying(100),
    cc_bic character varying(50),
    cc_num_masked character varying(30),
    is_fraud boolean,
    merch_eff_time character varying(200),
    merch_last_update_time character varying(200),
    merch_lat character varying(50),
    merch_long character varying(50),
    merch_zipcode character varying(20),
    merchant character varying(200),
    trans_date_trans_time timestamp without time zone,
    trans_num character varying(100),
    first_name character varying(100),
    last_name character varying(100),
    gender character varying(20),
    lat character varying(50),
    long character varying(50),
    city_pop integer,
    job character varying(200),
    dob date,
    address_street character varying(200),
    address_city character varying(100),
    address_state character varying(50),
    address_zip character varying(20)
);


ALTER TABLE payn_etl.cleaned_transactions OWNER TO payn_user;

--
-- Name: cleaned_transactions_partition; Type: TABLE; Schema: payn_etl; Owner: payn_user
--

CREATE TABLE payn_etl.cleaned_transactions_partition (
    unnamed_id character varying(50),
    amt numeric,
    category character varying(100),
    cc_bic character varying(50),
    cc_num_masked character varying(30),
    is_fraud boolean,
    merch_eff_time character varying(200),
    merch_last_update_time character varying(200),
    merch_lat character varying(50),
    merch_long character varying(50),
    merch_zipcode character varying(20),
    merchant character varying(200),
    trans_date_trans_time timestamp without time zone,
    trans_num character varying(100),
    first_name character varying(100),
    last_name character varying(100),
    gender character varying(20),
    lat character varying(50),
    long character varying(50),
    city_pop integer,
    job character varying(200),
    dob date,
    address_street_masked character varying(200),
    address_city character varying(100),
    address_state character varying(50),
    address_zip_masked character varying(20),
    ingestion_date date NOT NULL
)
PARTITION BY RANGE (ingestion_date);


ALTER TABLE payn_etl.cleaned_transactions_partition OWNER TO payn_user;

--
-- Name: cleaned_transactions_2025_11; Type: TABLE; Schema: payn_etl; Owner: payn_user
--

CREATE TABLE payn_etl.cleaned_transactions_2025_11 (
    unnamed_id character varying(50),
    amt numeric,
    category character varying(100),
    cc_bic character varying(50),
    cc_num_masked character varying(30),
    is_fraud boolean,
    merch_eff_time character varying(200),
    merch_last_update_time character varying(200),
    merch_lat character varying(50),
    merch_long character varying(50),
    merch_zipcode character varying(20),
    merchant character varying(200),
    trans_date_trans_time timestamp without time zone,
    trans_num character varying(100),
    first_name character varying(100),
    last_name character varying(100),
    gender character varying(20),
    lat character varying(50),
    long character varying(50),
    city_pop integer,
    job character varying(200),
    dob date,
    address_street_masked character varying(200),
    address_city character varying(100),
    address_state character varying(50),
    address_zip_masked character varying(20),
    ingestion_date date CONSTRAINT cleaned_transactions_partition_ingestion_date_not_null NOT NULL
);


ALTER TABLE payn_etl.cleaned_transactions_2025_11 OWNER TO payn_user;

--
-- Name: cleaned_transactions_id_seq; Type: SEQUENCE; Schema: payn_etl; Owner: payn_user
--

CREATE SEQUENCE payn_etl.cleaned_transactions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE payn_etl.cleaned_transactions_id_seq OWNER TO payn_user;

--
-- Name: cleaned_transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: payn_etl; Owner: payn_user
--

ALTER SEQUENCE payn_etl.cleaned_transactions_id_seq OWNED BY payn_etl.cleaned_transactions.id;


--
-- Name: cleaned_transactions_2025_11; Type: TABLE ATTACH; Schema: payn_etl; Owner: payn_user
--

ALTER TABLE ONLY payn_etl.cleaned_transactions_partition ATTACH PARTITION payn_etl.cleaned_transactions_2025_11 FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');


--
-- Name: cleaned_transactions id; Type: DEFAULT; Schema: payn_etl; Owner: payn_user
--

ALTER TABLE ONLY payn_etl.cleaned_transactions ALTER COLUMN id SET DEFAULT nextval('payn_etl.cleaned_transactions_id_seq'::regclass);


--
-- Name: cleaned_transactions cleaned_transactions_pkey; Type: CONSTRAINT; Schema: payn_etl; Owner: payn_user
--

ALTER TABLE ONLY payn_etl.cleaned_transactions
    ADD CONSTRAINT cleaned_transactions_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict ZfigvvY994YMTgviHJFqqhgVcALLu6cwVMLawaayq6oAmooiXtDoeY12Q7xRPkN

