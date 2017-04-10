sudo docker run -p 127.0.0.1:5433:5432 -v /home/mm41139/polymr/pgdata:/ext/pgdata --name polymr-postgres -e PGDATA=/ext/pgdata -e POSTGRES_PASSWORD=polymr -d postgres
