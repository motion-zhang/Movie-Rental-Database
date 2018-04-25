import java.sql.Array;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
    private static Properties configProps = new Properties();
    
    private static String imdbUrl;
    private static String customerUrl;
    
    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;
    
    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;
    
    // FOR SEARCH FUCNTION ///////////////////////////////////////////////////////////////
    private String _search_sql = "SELECT * FROM movie WHERE LOWER(name) LIKE LOWER(?) ORDER BY id";
    private PreparedStatement _search_statement;
    
    // for director
    private String _director_mid_sql = "SELECT y.* "
    + "FROM movie_directors x, directors y "
    + "WHERE x.mid = ? AND x.did = y.id";
    private PreparedStatement _director_mid_statement;
    
    // for actor
    private String _actor_mid_sql = "SELECT y.* "
    + "FROM casts x, actor y "
    + "WHERE x.mid = ? AND x.pid = y.id";
    private PreparedStatement _actor_mid_statement;
    
    // for availability
    private String _available_mid_sql = "SELECT x.* "
    + "FROM MovieStatus x "
    + "WHERE x.mid = ?";
    private PreparedStatement _available_mid_statement;
    
    // FOR FAST SEARCH FUNCTION ////////////////////////////////////////////////////////
    
    // for mid fast search and sensitive by "lower"
    private String _search_fast_sql = "SELECT * FROM movie WHERE LOWER(name) LIKE LOWER(?) ORDER BY id";
    private PreparedStatement _search_fast_statement;
    
    // for director fast search
    
    private Statement _director_fast_statement;
    
    // for actor fast search
    
    private Statement _actor_fast_statement;
    
    ////////////////////////////////////////////////////////////////////////////////////
    
    // return customer name from cid query
    private String _customer_name_sql = "SELECT fname, lname FROM Customers WHERE cid = ?";
    private PreparedStatement _customer_name_statement;
    
    // number of rentals on plan query
    private String _customer_rentals_onPlan_sql = "SELECT r.max_movies FROM Customers, RentalPlans as r "
    + "WHERE r.pid = Customers.pid AND cid = ?";
    private PreparedStatement _customer_rentals_onPlan_statement;
    
    // number of rented movies query
    private String _customer_movies_sql = "SELECT MoviesRented FROM Customers WHERE cid = ?";
    private PreparedStatement _customer_movies_statement;
    
    // does this plan exist query
    private String _plan_exists_sql = "SELECT * FROM RentalPlans WHERE pid = ?";
    private PreparedStatement _plan_exists_statement;
    
    // does this movie exist query
    private String _movie_exists_sql = "SELECT * FROM Movies WHERE mid = ?";
    private PreparedStatement _movie_exists_statement;
    
    // who rents this movie query
    private String _who_rents_sql = "SELECT cid"
    + "FROM MovieRentals m"
    + "WHERE m.mid = ? AND m.status = 'open'";
    private PreparedStatement _who_rents_statement;
    
    // all movies in rent by the given cid query
    private String _movie_in_rent_sql = "SELECT mid" + "FROM MovieRentals mr"
    + "WHERE mr.cid = ?" + "GROUP BY mr.cid";
    private PreparedStatement _movie_in_rent_statement;
    
    
    // TRANSACTION STATEMENTS
    private String _customer_login_sql = "SELECT * FROM customers WHERE login = ? AND password = ?";
    private PreparedStatement _customer_login_statement;
    
    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;
    
    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;
    
    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
    
    // to find all possible plan IDs
    private String _all_plans_sql = "Select * From RentalPlans";
    private PreparedStatement _all_plans_statement;
    
    //to find the customer and set their plan ID
    private String _customer_set_plan_sql = "UPDATE Customers SET pid = ? WHERE cid = ?";
    private PreparedStatement _customer_set_plan_statement;
    
    // print out customer's current plan
    private String _customer_planID_sql = "Select pid From Customers Where cid = ?";
    private PreparedStatement _customer_planID_statement;
    
    
    public Query() {
    }
    
    /**********************************************************/
    /* Connections to postgres databases */
    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        imdbUrl = configProps.getProperty("imdbUrl");
        customerUrl = configProps.getProperty("customerUrl");
        postgreSQLDriver = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");
        
        
        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();
        
        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                                            postgreSQLUser, // user
                                            postgreSQLPassword); // password
        
        _customer_db = DriverManager.getConnection(customerUrl, // database
                                                   postgreSQLUser, // user
                                                   postgreSQLPassword); // password
    }
    
    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }
    
    /**********************************************************/
    // PREPARE STATEMENTS
    public void prepareStatements() throws Exception {
        
        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
        _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
        _available_mid_statement = _customer_db.prepareStatement(_available_mid_sql);
        
        _search_fast_statement = _imdb.prepareStatement(_search_fast_sql);
        
        
        _customer_name_statement = _customer_db.prepareStatement(_customer_name_sql);
        _customer_rentals_onPlan_statement = _customer_db.prepareStatement(_customer_rentals_onPlan_sql);
        _customer_movies_statement = _customer_db.prepareStatement(_customer_movies_sql);
        
        _plan_exists_statement = _customer_db.prepareStatement(_plan_exists_sql);
        _movie_exists_statement = _customer_db.prepareStatement(_movie_exists_sql);
        _who_rents_statement = _customer_db.prepareStatement(_who_rents_sql);
        
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
        
        // transaction 3
        _all_plans_statement = _customer_db.prepareStatement(_all_plans_sql);
        _customer_set_plan_statement = _customer_db.prepareStatement(_customer_set_plan_sql);
        _customer_planID_statement = _customer_db.prepareStatement(_customer_planID_sql);
        
        // movie in rent problem
        _movie_in_rent_statement = _customer_db.prepareStatement(_movie_in_rent_sql);
    }
    
    /**********************************************************/
    /* suggested helper functions  */
    
    // returns a customer's remaining allowed rentals
    public int helper_compute_remaining_rentals(int cid) throws Exception {
        int moviesOnPlan = 0;
        _customer_rentals_onPlan_statement.clearParameters();
        _customer_rentals_onPlan_statement.setInt(1, cid);
        ResultSet rental_set = _customer_rentals_onPlan_statement.executeQuery();
        while (rental_set.next()) {
            moviesOnPlan = rental_set.getInt(1);
        }
        
        int moviesTakenOut = 0;
        _customer_movies_statement.clearParameters();
        _customer_movies_statement.setInt(1, cid);
        ResultSet movies_set = _customer_movies_statement.executeQuery();
        while (movies_set.next()) {
            moviesTakenOut = movies_set.getInt(1);
        }
        return moviesOnPlan - moviesTakenOut;
    }
    
    // returns customer name from given customer id
    public String helper_compute_customer_name(int cid) throws Exception {
        String fname = "";
        String lname = "";
        /* you find  the first + last name of the current customer */
        _customer_name_statement.clearParameters();
        _customer_name_statement.setInt(1, cid);
        ResultSet customer_set = _customer_name_statement.executeQuery();
        while (customer_set.next()) {
            fname = customer_set.getString(1);
            lname = customer_set.getString(2);
        }
        return (fname + " " + lname);
    }
    
    // check if given plan id exists
    public boolean helper_check_plan(int plan_id) throws Exception {
        boolean exists = false;
        _plan_exists_statement.clearParameters();
        _plan_exists_statement.setInt(1, plan_id);
        ResultSet plan_set = _plan_exists_statement.executeQuery();
        while (plan_set.next()) {
            exists = true;
            break;
        }
        return exists;
    }
    
    // check if given movie id exists
    public boolean helper_check_movie(int mid) throws Exception {
        boolean exists = false;
        _movie_exists_statement.clearParameters();
        _movie_exists_statement.setInt(1, mid);
        ResultSet movie_set = _movie_exists_statement.executeQuery();
        while (movie_set.next()) {
            exists = true;
            break;
        }
        return exists;
    }
    
    // check who is renting the given movie
    private int helper_who_has_this_movie(int mid) throws Exception {
        int renter_cid = -1;
        _who_rents_statement.clearParameters();
        _who_rents_statement.setInt(1, mid);
        ResultSet who_rents_set = _who_rents_statement.executeQuery();
        while (who_rents_set.next()) {
            renter_cid = who_rents_set.getInt(1);
            break;
        }
        return renter_cid;
    }
    
    /**********************************************************/
    /* login transaction: invoked only once, when the app is started */
    public int transaction_login(String name, String password) throws Exception {
        int cid;
        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1, name);
        _customer_login_statement.setString(2, password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        return (cid);
    }
    
    // print customer's name and reamining number of rentals
    public void transaction_personal_data(int cid) throws Exception {
        String name = this.helper_compute_customer_name(cid);
        int custMovies = this.helper_compute_remaining_rentals(cid);
        System.out.println("User: " + name + ", Movies Left: " + Integer.toString(custMovies));
    }
    
    /**********************************************************/
    /* main functions in this project: */
    
    // BASIC SEARCH FUNCTION
    public void transaction_search(int cid, String movie_title)
    throws Exception {
        
        // movie information
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');
        
        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                               + movie_set.getString(2) + " YEAR: "
                               + movie_set.getString(3));
            
            // director information
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                                   + " " + director_set.getString(2));
            }
            
            // actor information
            _actor_mid_statement.clearParameters();
            _actor_mid_statement.setInt(1, mid);
            
            ResultSet actor_set = _actor_mid_statement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\tActor: " + actor_set.getString(3)
                                   + " " + actor_set.getString(2));
            }
            actor_set.close();
            
            // availability information
            _available_mid_statement.clearParameters();
            _available_mid_statement.setInt(1, mid);
            ResultSet available_set = _available_mid_statement.executeQuery();
            int availability = 1;
            while (available_set.next()) {
                if (available_set.getInt(3) == 1) {
                    availability = 3;
                    break;
                } else {
                    if (cid == available_set.getInt(2)) {
                        availability = 2;
                        break;
                    } else {
                        availability = 1;
                        break;
                    }
                }
            }
            if (availability == 1) {
                System.out.println("\t\tStatus: Unavailable");
            } else if (availability == 2) {
                System.out.println("\t\tStatus: You are renting this");
            } else {
                System.out.println("\t\tStatus: Available");
            }
            available_set.close();
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////
    
    
    public void transaction_choose_plan(int cid, int pid) throws Exception {
        int PLANID = -1;
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
        /* remember to enforce consistency ! */
        _begin_transaction_read_write_statement.execute();
        if (this.helper_check_plan(pid)) {
            _customer_set_plan_statement.clearParameters();
            _customer_set_plan_statement.setInt(2, cid);
            _customer_set_plan_statement.setInt(1, pid);
            _customer_set_plan_statement.executeUpdate();
            _customer_planID_statement.clearParameters();
            _customer_planID_statement.setInt(1, cid);
            ResultSet custPlan = _customer_planID_statement.executeQuery();
            while (custPlan.next()) {
                PLANID = custPlan.getInt(1);
            }
            System.out.println("Successful Update. The new PlanID is: " + Integer.toString(PLANID));
        }
        _commit_transaction_statement.execute();
    }
    
    public void transaction_list_plans() throws Exception {
        _begin_transaction_read_write_statement.execute();
        ArrayList<ArrayList<String>> plans = new ArrayList<ArrayList<String>>();
        String result = "";
        _all_plans_statement.clearParameters();
        ResultSet plan_set = _all_plans_statement.executeQuery();
        int i = 0;
        while (plan_set.next()) {
            plans.add(new ArrayList<String>());
            String planID = Integer.toString(plan_set.getInt(1));
            plans.get(i).add(planID);
            String planName = plan_set.getString(2);
            plans.get(i).add(planName);
            String maxMovies = Integer.toString(plan_set.getInt(3));
            plans.get(i).add(maxMovies);
            String fee = Integer.toString(plan_set.getInt(4));
            plans.get(i).add(fee);
            i++;
        }
        for (int j = 0; j < plans.size(); j++) {
            result = result + "PlanID: " + plans.get(j).get(0) + " Plan-Name: " + plans.get(j).get(1) +
            " Max-Movies: " + plans.get(j).get(2) + " Plan-Fee: "
            + plans.get(j).get(3) + "\n";
        }
        _commit_transaction_statement.execute();
        System.out.println(result);
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
        _movie_in_rent_statement.clearParameters();
        _movie_in_rent_statement.setInt(1, cid);
        ResultSet movie_set = _movie_in_rent_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid);
        }
        movie_set.close();
        
    }
    
    public void transaction_rent(int cid, int mid) throws Exception {
        /* rend the movie mid to the customer cid */
        /* remember to enforce consistency ! */
        _movie_in_rent_statement.clearParameters();
        _movie_in_rent_statement.setInt(1, cid);
        ResultSet movie_set = _movie_in_rent_statement.executeQuery();
        while (movie_set.next()) {
            int mmid = movie_set.getInt(1);
            if (mmid == mid) {
                throw new IllegalArgumentException("The movie is already rented by this customer");
            }
        }
        _begin_transaction_read_write_statement.execute();
        
    }
    
    public void transaction_return(int cid, int mid) throws Exception {
        /* return the movie mid by the customer cid */
    }
    
    
    // NEEDS TO BE DONE
    public void transaction_fast_search(int cid, String movie_title)
    throws Exception {
        _search_fast_statement.clearParameters();
        _search_fast_statement.setString(1, '%' + movie_title + '%');
        
        _director_fast_statement = _imdb.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                         ResultSet.CONCUR_READ_ONLY);
        _actor_fast_statement =  _imdb.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                                       ResultSet.CONCUR_READ_ONLY);
        
        
        
        
        // for director fast search
        String _director_fast_sql = "SELECT x.id, y.fname, y.lname FROM movie x, directors y, movie_directors z "
        + "WHERE LOWER(x.name) LIKE LOWER('%" + movie_title + "%') AND x.id = z.mid AND z.did = y.id ORDER BY x.id";
        
        
        // for actor fast search
        String _actor_fast_sql = "SELECT x.id, y.fname, y.lname FROM movie x, actor y, casts z "
        + "WHERE LOWER(x.name) LIKE LOWER('%" + movie_title + "%') AND x.id = z.mid AND z.pid = y.id ORDER BY x.id";
        
        
        ResultSet movieSet = _search_fast_statement.executeQuery();
        ResultSet directorSet = _director_fast_statement.executeQuery(_director_fast_sql);
        ResultSet actorSet = _actor_fast_statement.executeQuery(_actor_fast_sql);
        
        
        while (movieSet.next()) {
            int mid = movieSet.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                               + movieSet.getString(2) + " YEAR: "
                               + movieSet.getString(3));
            
            while (directorSet.next()) {
                if (directorSet.getInt(1) == mid) {
                    System.out.println("\t\tDirector: "
                                       + directorSet.getString(3) + " "
                                       + directorSet.getString(2));
                } else {
                    directorSet.previous();
                    break;
                }
            }
            
            while (actorSet.next()) {
                if (actorSet.getInt(1) == mid) {
                    System.out.println("\t\tActor: "
                                       + actorSet.getString(3) + " "
                                       + actorSet.getString(2));
                } else {
                    actorSet.previous();
                    break;
                }
            }
            
            
        }
    }
}
