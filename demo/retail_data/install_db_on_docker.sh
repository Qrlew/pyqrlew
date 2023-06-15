# install Postgres image
docker run --name some-postgres -e POSTGRES_PASSWORD=1234 -d postgres
docker cp demo/retail_data/ some-postgres:.
docker exec some-postgres psql -U postgres -f demo/retail_data/install_db.sql
