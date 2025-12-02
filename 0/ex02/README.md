.env must be in ./srcs/

POSTGRES_USER=kbaridon
POSTGRES_PASSWORD=mysecretpassword
POSTGRES_DB=piscineds

To see how to access PostgreSQL: make help


--
To create the table:

CREATE TABLE data_2022_oct (
	event_time timestamptz,
	event_type text,
	product_id	 integer,
	price decimal(10,2),
	user_id bigint,
	user_session uuid
);

To see it:

\dt