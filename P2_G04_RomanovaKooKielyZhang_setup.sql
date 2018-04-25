CREATE TABLE RentalPlans(
pid integer,
name varchar(50) UNIQUE NOT NULL CHECK (name = 'Basic' or name = 'Rental Plus' or name = 'Super'),
max_movies int NOT NULL,
fee int NOT NULL,
PRIMARY KEY (pid)
);

CREATE TABLE Customers(
cid integer NOT NULL UNIQUE,
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
dateRented date NOT NULL,
dateReturned date,
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
INSERT INTO Customers VALUES (1, 'george', '123', 'George', 'Ford', 1, '1111111111', 'george@gmail.com', '10 Tenth St.', 'Boston', 'MA', 0);
INSERT INTO Customers VALUES (2, 'mary', '123', 'Mary', 'Jones', 2, '2222222222', 'mary@yahoo.com', '1 Beet St.', 'Nashua', 'NH', 6);
INSERT INTO MovieRentals VALUES (1, 1, 32573, DATE('2018-01-15'), DATE('2018-01-16'), 'closed');
INSERT INTO MovieRentals VALUES (2, 2, 107448, DATE('2018-02-15'), NULL, 'open');
INSERT INTO MovieRentals VALUES (3, 2, 141877, DATE('2018-03-15'), NULL, 'open');
INSERT INTO MovieRentals VALUES (4, 2, 166994, DATE('2018-03-16'), NULL, 'open');
INSERT INTO MovieRentals VALUES (5, 2, 167917, DATE('2018-04-01'), NULL, 'open');
INSERT INTO MovieRentals VALUES (6, 2, 196668, DATE('2018-04-01'), NULL, 'open');
INSERT INTO MovieRentals VALUES (7, 2, 215261, DATE('2018-04-02'), NULL, 'open');
INSERT INTO MovieRentals VALUES (8, 2, 224787, DATE('2018-04-03'), DATE('2018-04-04'), 'closed');

INSERT INTO MovieStatus VALUES (32573, -1, 1);
INSERT INTO MovieStatus VALUES (107448, 2, 0);
INSERT INTO MovieStatus VALUES (141877, 2, 0);
INSERT INTO MovieStatus VALUES (166994, 2, 0);
INSERT INTO MovieStatus VALUES (167917, 2, 0);
INSERT INTO MovieStatus VALUES (196668, 2, 0);
INSERT INTO MovieStatus VALUES (215261, 2, 0);
INSERT INTO MovieStatus VALUES (224787, -1, 1);














