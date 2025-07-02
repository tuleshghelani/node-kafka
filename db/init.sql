-- Users table for authentication
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Enquiry master table
CREATE TABLE public.enquiry_master (
	id bigserial NOT NULL,
	"name" varchar NULL,
	email varchar NULL,
	mobilenumber varchar NULL,
	pet_name varchar NULL,
	type_of_pet varchar NULL,
	pet_age varchar NULL,
	like_to_buy varchar NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	status varchar(1) DEFAULT 'P'::character varying NULL,
	CONSTRAINT enquiry_master_pkey PRIMARY KEY (id)
);

-- DROP TABLE public.contact;
CREATE TABLE public.contact (
	id bigserial NOT NULL,
	full_name varchar NOT NULL,
	email varchar NOT NULL,
	subject varchar NOT NULL,
	mobilenumber varchar NULL,
	message varchar NULL,
	status varchar DEFAULT 'P'::character varying NULL,
	created_at varchar DEFAULT now() NULL,
	company_name varchar NULL,
	designation varchar NULL,
	country varchar NULL,
	CONSTRAINT contact_pk PRIMARY KEY (id)
);