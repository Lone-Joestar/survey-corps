--
-- PostgreSQL database dump
--

\restrict WQHskY9gUtq9G9dbD9Wr3nSjHOikUTJuUoj1tS7sAW4AqIExz5KsK2fO7BDZ46f

-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
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
-- Name: dim_customer; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_customer (
    customer_sk integer NOT NULL,
    customer_id integer NOT NULL,
    name character varying(60) NOT NULL,
    email character varying(50) NOT NULL,
    country character varying(50) NOT NULL,
    age integer NOT NULL,
    signup_date date NOT NULL,
    marketing_opt_in boolean NOT NULL,
    valid_from timestamp without time zone NOT NULL,
    valid_to timestamp without time zone NOT NULL,
    is_current boolean NOT NULL
);


--
-- Name: dim_customer_customer_sk_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_customer_customer_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_customer_customer_sk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_customer_customer_sk_seq OWNED BY public.dim_customer.customer_sk;


--
-- Name: dim_order_attributes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_order_attributes (
    order_attributes_sk integer NOT NULL,
    payment_method character varying(50) NOT NULL,
    country character varying(50) NOT NULL,
    device character varying(50) NOT NULL,
    source character varying(50) NOT NULL
);


--
-- Name: dim_order_attributes_order_attributes_sk_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.dim_order_attributes_order_attributes_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dim_order_attributes_order_attributes_sk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.dim_order_attributes_order_attributes_sk_seq OWNED BY public.dim_order_attributes.order_attributes_sk;


--
-- Name: dim_product; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dim_product (
    product_id integer NOT NULL,
    category character varying(50) NOT NULL,
    name character varying(50) NOT NULL,
    price_usd numeric(10,2) NOT NULL,
    cost_usd numeric(10,2) NOT NULL,
    margin_usd numeric(10,2) NOT NULL
);


--
-- Name: fact_order_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_order_items (
    order_item_sk integer NOT NULL,
    order_id integer NOT NULL,
    product_id integer NOT NULL,
    unit_price_usd numeric(10,2) NOT NULL,
    quantity integer NOT NULL,
    line_total_usd numeric(10,2) NOT NULL
);


--
-- Name: fact_order_items_order_item_sk_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.fact_order_items_order_item_sk_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: fact_order_items_order_item_sk_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.fact_order_items_order_item_sk_seq OWNED BY public.fact_order_items.order_item_sk;


--
-- Name: fact_orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.fact_orders (
    order_id integer NOT NULL,
    customer_sk integer NOT NULL,
    order_attributes_sk integer NOT NULL,
    order_time timestamp without time zone NOT NULL,
    discount_pct numeric(10,2) NOT NULL,
    sub_total_usd numeric(10,2) NOT NULL,
    total_usd numeric(10,2) NOT NULL
);


--
-- Name: rt_abandoned_carts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.rt_abandoned_carts (
    session_id bigint NOT NULL,
    window_start timestamp without time zone NOT NULL,
    window_end timestamp without time zone NOT NULL,
    cart_adds bigint,
    total_events bigint
);


--
-- Name: rt_funnel_hourly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.rt_funnel_hourly (
    window_start timestamp without time zone NOT NULL,
    window_end timestamp without time zone NOT NULL,
    event_type text NOT NULL,
    events bigint,
    sessions bigint
);


--
-- Name: dim_customer customer_sk; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_customer ALTER COLUMN customer_sk SET DEFAULT nextval('public.dim_customer_customer_sk_seq'::regclass);


--
-- Name: dim_order_attributes order_attributes_sk; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_order_attributes ALTER COLUMN order_attributes_sk SET DEFAULT nextval('public.dim_order_attributes_order_attributes_sk_seq'::regclass);


--
-- Name: fact_order_items order_item_sk; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_order_items ALTER COLUMN order_item_sk SET DEFAULT nextval('public.fact_order_items_order_item_sk_seq'::regclass);


--
-- Name: dim_customer dim_customer_customer_id_valid_from_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_customer
    ADD CONSTRAINT dim_customer_customer_id_valid_from_key UNIQUE (customer_id, valid_from);


--
-- Name: dim_customer dim_customer_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_customer
    ADD CONSTRAINT dim_customer_pkey PRIMARY KEY (customer_sk);


--
-- Name: dim_order_attributes dim_order_attributes_payment_method_country_device_source_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_order_attributes
    ADD CONSTRAINT dim_order_attributes_payment_method_country_device_source_key UNIQUE (payment_method, country, device, source);


--
-- Name: dim_order_attributes dim_order_attributes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_order_attributes
    ADD CONSTRAINT dim_order_attributes_pkey PRIMARY KEY (order_attributes_sk);


--
-- Name: dim_product dim_product_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dim_product
    ADD CONSTRAINT dim_product_pkey PRIMARY KEY (product_id);


--
-- Name: fact_order_items fact_order_items_order_id_product_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_order_items
    ADD CONSTRAINT fact_order_items_order_id_product_id_key UNIQUE (order_id, product_id);


--
-- Name: fact_order_items fact_order_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_order_items
    ADD CONSTRAINT fact_order_items_pkey PRIMARY KEY (order_item_sk);


--
-- Name: fact_orders fact_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_orders
    ADD CONSTRAINT fact_orders_pkey PRIMARY KEY (order_id);


--
-- Name: rt_abandoned_carts rt_abandoned_carts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rt_abandoned_carts
    ADD CONSTRAINT rt_abandoned_carts_pkey PRIMARY KEY (session_id, window_start);


--
-- Name: rt_funnel_hourly rt_funnel_hourly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rt_funnel_hourly
    ADD CONSTRAINT rt_funnel_hourly_pkey PRIMARY KEY (window_start, event_type);


--
-- Name: dim_customer_current_uq; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX dim_customer_current_uq ON public.dim_customer USING btree (customer_id) WHERE (is_current = true);


--
-- Name: fact_order_items fact_order_items_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_order_items
    ADD CONSTRAINT fact_order_items_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.fact_orders(order_id);


--
-- Name: fact_order_items fact_order_items_product_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_order_items
    ADD CONSTRAINT fact_order_items_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.dim_product(product_id);


--
-- Name: fact_orders fact_orders_customer_sk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_orders
    ADD CONSTRAINT fact_orders_customer_sk_fkey FOREIGN KEY (customer_sk) REFERENCES public.dim_customer(customer_sk);


--
-- Name: fact_orders fact_orders_order_attributes_sk_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.fact_orders
    ADD CONSTRAINT fact_orders_order_attributes_sk_fkey FOREIGN KEY (order_attributes_sk) REFERENCES public.dim_order_attributes(order_attributes_sk);


--
-- PostgreSQL database dump complete
--

\unrestrict WQHskY9gUtq9G9dbD9Wr3nSjHOikUTJuUoj1tS7sAW4AqIExz5KsK2fO7BDZ46f

