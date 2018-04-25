CREATE TABLE RentalPlans(
pid integer,
name varchar(50) UNIQUE NOT NULL CHECK (name = 'Basic' or name = 'Rental Plus' or name = 'Super'),
max_movies int NOT NULL,
fee int NOT NULL,
PRIMARY KEY (pid)
);

CREATE TABLE Customers(
cid integer,
login varchar(50) NOT NULL UNIQUE,
password varchar(50) NOT NULL,
fname varchar(50),
lname varchar(50),
pid integer,
phone varchar(10),
email varchar(50),
street varchar(50),
city varchar(25),
state varchar(25),
MoviesRented int,
PRIMARY KEY (cid),
FOREIGN KEY (pid) REFERENCES RentalPlans(pid)
);

CREATE TABLE MovieRentals(
rid int, 
cid int NOT NULL,
mid int NOT NULL,
dateRented int NOT NULL,
dateReturned int,
status varchar(10) CHECK (status = 'open' or status = 'closed'),
PRIMARY KEY (rid),
FOREIGN KEY (cid) REFERENCES Customers(cid));

CREATE TABLE MovieStatus(
mid int UNIQUE NOT NULL,
cid int NOT NULL,
status int CHECK (status = 0 or status = 1));

INSERT INTO RentalPlans VALUES (1, 'Basic', 5, 5);
INSERT INTO RentalPlans VALUES (2, 'Rental Plus', 10, 10);
INSERT INTO RentalPlans VALUES (3, 'Super', 20, 15);
INSERT INTO Customers VALUES (1, 'george', '123', 'George', 'Ford', 1, '1111111111', 'george@gmail.com', '10 Tenth St.', 'Boston', 'MA');
INSERT INTO MovieRentals VALUES (1, 1, 32573, 10, 12, 'closed');
INSERT INTO MovieStatus VALUES (32573, -1, 1);
INSERT INTO MovieStatus VALUES (107448, 1, 0);














