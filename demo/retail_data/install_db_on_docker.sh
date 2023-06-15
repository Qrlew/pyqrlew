# install Postgres image
docker run --name pyqrlew-test -e POSTGRES_PASSWORD=pyqrlew-test  -p 5432:5432 -d postgres
docker cp demo/retail_data pyqrlew-test:.
docker exec pyqrlew-test psql -U postgres -f demo/retail_data/install_db.sql