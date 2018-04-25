import java.util.ArrayList;
import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.FileInputStream;
import java.util.Calendar;
import java.sql.Date;
import java.sql.Statement;
import java.sql.Array;

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
    
    
    // CID AS INPUT
    // return customer info from cid query
    private String _customer_info_sql = "SELECT * FROM Customers WHERE cid = ?";
    private PreparedStatement _customer_info_statement;
    
    // return customer plan from cid query
    private String _customer_plan_sql = "SELECT x.name FROM RentalPlans x, Customers y WHERE cid = ? AND x.pid = y.pid";
    private PreparedStatement _customer_plan_statement;
    
    // number of rentals on plan query
    private String _customer_rentals_onPlan_sql = "SELECT r.max_movies FROM Customers, RentalPlans as r "
    + "WHERE r.pid = Customers.pid AND cid = ?";
    private PreparedStatement _customer_rentals_onPlan_statement;
    
    
    // MID AS INPUT
    // movie information
    private String _movie_info_sql = "SELECT * FROM Movies WHERE mid = ?";
    private PreparedStatement _movie_info_statement;
    
    // movie rental information
    private String _movie_status_sql = "SELECT * FROM MovieStatus WHERE mid = ?";
    private PreparedStatement _movie_status_statement;
    
    // who rents this movie query
    private String _who_rents_sql = "SELECT cid "
    + "FROM MovieRentals "
    + "WHERE mid = ? AND status = 'open'";
    private PreparedStatement _who_rents_statement;
    
    
    // PID AS INPUT
    // does this plan exist query
    private String _plan_exists_sql = "SELECT * FROM RentalPlans WHERE pid = ?";
    private PreparedStatement _plan_exists_statement;
    
    // number of max movies for a plan
    private String _max_movies_sql = "SELECT max_movies FROM RentalPlans WHERE pid = ?";
    private PreparedStatement _max_movies_statement;
    
    
    // FOR PLAN TRANSACTION //////////////////////////////////////////////////////////////////////////////////
    
    // to find all possible plan IDS
    private String _all_plans_sql = "SELECT * FROM RentalPlans";
    private PreparedStatement _all_plans_statement;
    
    //to find the customer and set their plan ID
    private String _customer_set_plan_sql = "UPDATE Customers SET pid = ? WHERE cid = ?";
    private PreparedStatement _customer_set_plan_statement;
    
    // print out customer's current plan
    private String _customer_planID_sql = "SELECT pid FROM Customers WHERE cid = ?";
    private PreparedStatement _customer_planID_statement;
    
    
    // FOR RENT TRANSACTION //////////////////////////////////////////////////////////////
    // update customer number of movies
    private String _customer_update_movies_sql = "UPDATE Customers SET MoviesRented = ? WHERE cid = ?";
    private PreparedStatement _customer_update_movies_statement;
    
    private String _movie_status_update_sql = "UPDATE MovieStatus SET status = ?, CID = ? WHERE mid = ?";
    private PreparedStatement _movie_status_update_statement;
    
    private String _movie_rental_create_sql = "INSERT INTO MovieRentals VALUES (?, ?, ?, ?, NULL, 'open')";
    private PreparedStatement _movie_rental_create_statement;
    
    private String _max_rid_sql = "SELECT MAX(rid) FROM MovieRentals";
    private PreparedStatement _max_rid_statement;
    
    // FOR RETURN TRANSACTION //////////////////////////////////////////////////////////////////
    // update customers amount of movie rentals
    private String _update_customer_movies = "UPDATE Customers Set MoviesRented = MoviesRented - 1 Where cid = ?";
    private PreparedStatement _update_customer_movies_statement;
    
    // update the movie's return date
    private String _update_movie_return_sql = "Update MovieRentals "
    + "Set dateReturned = ?, status = 'closed' Where mid = ?";
    private PreparedStatement _update_movie_return_statement;
    
    // update the movies status
    private String _update_movie_status_sql = "Update MovieStatus Set cid = -1, status = 1 "
    + "Where mid = ?";
    private PreparedStatement _update_movie_status_statement;
    
    
    // TRANSACTION STATEMENTS ///////////////////////////////////////////////////////////////////////////////
    private String _customer_login_sql = "SELECT * FROM customers WHERE login = ? AND password = ?";
    private PreparedStatement _customer_login_statement;
    
    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;
    
    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;
    
    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;
    
    public Query() {
    }
    
    /**********************************************************/
    /* Connections to postgres databases */
    
    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
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
        
        
        _customer_info_statement = _customer_db.prepareStatement(_customer_info_sql);
        _customer_plan_statement = _customer_db.prepareStatement(_customer_plan_sql);
        _customer_rentals_onPlan_statement = _customer_db.prepareStatement(_customer_rentals_onPlan_sql);
        _customer_update_movies_statement = _customer_db.prepareStatement(_customer_update_movies_sql);
        
        _plan_exists_statement = _customer_db.prepareStatement(_plan_exists_sql);
        _movie_info_statement = _imdb.prepareStatement(_movie_info_sql);
        _who_rents_statement = _customer_db.prepareStatement(_who_rents_sql);
        _max_movies_statement = _customer_db.prepareStatement(_max_movies_sql);
        _movie_status_statement = _customer_db.prepareStatement(_movie_status_sql);
        _movie_status_update_statement = _customer_db.prepareStatement(_movie_status_update_sql);
        _movie_rental_create_statement = _customer_db.prepareStatement(_movie_rental_create_sql);
        _max_rid_statement = _customer_db.prepareStatement(_max_rid_sql);
        
        _update_customer_movies_statement = _customer_db.prepareStatement(_update_customer_movies);
        _update_movie_return_statement = _customer_db.prepareStatement(_update_movie_return_sql);
        _update_movie_status_statement = _customer_db.prepareStatement(_update_movie_status_sql);
        
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);
        
        _all_plans_statement = _customer_db.prepareStatement(_all_plans_sql);
        _customer_set_plan_statement = _customer_db.prepareStatement(_customer_set_plan_sql);
        _customer_planID_statement = _customer_db.prepareStatement(_customer_planID_sql);
    }
    
    /**********************************************************/
    /* suggested helper functions  */
    
    // returns a customer's current rentals
    public int helper_compute_current_rentals(int cid) throws Exception {
        int moviesOnPlan = 0;
        _customer_info_statement.clearParameters();
        _customer_info_statement.setInt(1, cid);
        ResultSet rental_set = _customer_info_statement.executeQuery();
        while(rental_set.next()) {
            moviesOnPlan = rental_set.getInt(12);
        }
        return moviesOnPlan;
    }
    
    // returns a customer's remaining allowed rentals
    public int helper_compute_remaining_rentals(int cid) throws Exception {
        int moviesOnPlan = this.helper_max_movies(this.helper_compute_customer_pid(cid));
        int moviesTakenOut = this.helper_compute_current_rentals(cid);
        return moviesOnPlan - moviesTakenOut;
    }
    
    // returns customer name from given customer id
    public String helper_compute_customer_name(int cid) throws Exception {
        String fname = "";
        String lname = "";
        /* you find  the first + last name of the current customer */
        _customer_info_statement.clearParameters();
        _customer_info_statement.setInt(1, cid);
        ResultSet customer_set = _customer_info_statement.executeQuery();
        while (customer_set.next()) {
            fname = customer_set.getString(4);
            lname = customer_set.getString(5);
        }
        return (fname + " " + lname);
    }
    
    // returns customer pid from given customer id
    public int helper_compute_customer_pid(int cid) throws Exception {
        int pid = 0;
        _customer_planID_statement.clearParameters();
        _customer_planID_statement.setInt(1, cid);
        ResultSet customer_set = _customer_planID_statement.executeQuery();
        while (customer_set.next()) {
            pid = customer_set.getInt(1);
        }
        return (pid);
    }
    
    
    // returns customer plan from given customer id
    public String helper_compute_customer_plan(int cid) throws Exception {
        String plan = "";
        _customer_plan_statement.clearParameters();
        _customer_plan_statement.setInt(1, cid);
        ResultSet customer_set = _customer_plan_statement.executeQuery();
        while (customer_set.next()) {
            plan = customer_set.getString(1);
        }
        return (plan);
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
    
    // return max movies from given plan id
    public int helper_max_movies(int pid) throws Exception {
        int max = 0;
        _max_movies_statement.clearParameters();
        _max_movies_statement.setInt(1,  pid);
        ResultSet max_movies = _max_movies_statement.executeQuery();
        while (max_movies.next()) {
            max = max_movies.getInt(1);
            break;
        }
        return max;
    }
    
    // check if given movie id exists
    public boolean helper_check_movie(int mid) throws Exception {
        boolean exists = false;
        _movie_info_statement.clearParameters();
        _movie_info_statement.setInt(1, mid);
        ResultSet movie_set = _movie_info_statement.executeQuery();
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
    
    // check if a movie is available
    private boolean helper_movie_available(int mid) throws Exception {
        int available = -1;
        _movie_status_statement.clearParameters();
        _movie_status_statement.setInt(1,mid);
        ResultSet movie_set = _movie_status_statement.executeQuery();
        while (movie_set.next()) {
            available = movie_set.getInt(3);
        }
        if(available == 0) {
            return false;
        }
        else {
            return true;
        }
    }
    
    // returns the current max rid in MovieRentals
    private int helper_max_rid() throws Exception {
        int max = 0;
        _max_rid_statement.clearParameters();
        ResultSet max_rid = _max_rid_statement.executeQuery();
        while(max_rid.next()) {
            max = max_rid.getInt(1);
        }
        return max;
    }
    
    /**********************************************************/
    /* login transaction: invoked only once, when the app is started */
    public int transaction_login(String name, String password) throws Exception {
        int cid;
        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,name);
        _customer_login_statement.setString(2,password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        return(cid);
    }
    
    // print customer's name, plan, and reamining number of rentals
    public void transaction_personal_data(int cid) throws Exception {
        String name = this.helper_compute_customer_name(cid);
        String plan = this.helper_compute_customer_plan(cid);
        int custMovies = this.helper_compute_remaining_rentals(cid);
        System.out.println("User: " + name + ", Plan: " + plan +  ", Movies Left: " + Integer.toString(custMovies));
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
            while(available_set.next()) {
                if(available_set.getInt(3) == 1) {
                    availability = 3;
                    break;
                }
                else {
                    if(cid == available_set.getInt(2)) {
                        availability = 2;
                        break;
                    }
                    else {
                        availability = 1;
                        break;
                    }
                }
            }
            if (availability == 1) {
                System.out.println("\t\tStatus: Unavailable");
            }
            else if (availability == 2) {
                System.out.println("\t\tStatus: You are renting this");
            }
            else {
                System.out.println("\t\tStatus: Available");
            }
            available_set.close();
        }
    }
    ////////////////////////////////////////////////////////////////////////////////////
    
    // transaction that changes customer plan if possible
    public void transaction_choose_plan(int cid, int pid) throws Exception {
        int PLANID = -1;
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
        /* remember to enforce consistency ! */
        _begin_transaction_read_write_statement.execute();
        if(this.helper_check_plan(pid)) {
            if(this.helper_compute_customer_pid(cid) != pid) {
                if(this.helper_compute_current_rentals(cid) <= this.helper_max_movies(pid)) {
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
                    _commit_transaction_statement.execute();
                    System.out.println("Successful Update. The new PlanID is: " + Integer.toString(PLANID));
                }
                else {
                    _rollback_transaction_statement.execute();
                    System.out.println("Unsucessful Update. Too many movies currently rented to switch to this plan: ");
                }
            }
            else {
                _rollback_transaction_statement.execute();
                System.out.println("You are currently enrolled in this plan!");
            }
        }
    }
    
    // lists all the available plans
    public void transaction_list_plans() throws Exception {
        _begin_transaction_read_write_statement.execute();
        ArrayList<ArrayList<String>> plans = new ArrayList<ArrayList<String>>();
        String result = "";
        _all_plans_statement.clearParameters();
        ResultSet plan_set = _all_plans_statement.executeQuery();
        int i = 0;
        while(plan_set.next()) {
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
        for(int j = 0; j < plans.size(); j++) {
            result = result + "PlanID: " + plans.get(j).get(0) + " Plan-Name: " + plans.get(j).get(1) +
            " Max-Movies: "+ plans.get(j).get(2) + " Plan-Fee: "
            + plans.get(j).get(3) + "\n";
        }
        _commit_transaction_statement.execute();
        System.out.println(result);
    }
    
    // ???????????????/ DO WE NEED THIS
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
    }
    
    
    public void transaction_rent(int cid, int mid) throws Exception {
        _begin_transaction_read_write_statement.execute();
        boolean available = this.helper_movie_available(mid);
        if(!available) {
            if(this.helper_who_has_this_movie(mid) == cid) {
                _rollback_transaction_statement.execute();
                System.out.println("You are currently renting this movie.");
            }
            else {
                _rollback_transaction_statement.execute();
                System.out.println("Sorry, someone is currently renting this movie.");
            }
        }
        
        else {
            if (this.helper_compute_remaining_rentals(cid) == 0) {
                _rollback_transaction_statement.execute();
                System.out.println("Sorry, you are currenting renting the maximum number of movies for your plan.");
            }
            else {
                _customer_update_movies_statement.clearParameters();
                _customer_update_movies_statement.setInt(1, this.helper_compute_current_rentals(cid) + 1);
                _customer_update_movies_statement.setInt(2, cid);
                _customer_update_movies_statement.executeUpdate();
                
                _movie_status_update_statement.clearParameters();
                _movie_status_update_statement.setInt(1, 0);
                _movie_status_update_statement.setInt(3, mid);
                _movie_status_update_statement.setInt(2, cid);
                
                _movie_status_update_statement.executeUpdate();
                
                Calendar currenttime = Calendar.getInstance();
                Date sqldate = new Date((currenttime.getTime()).getTime());
                
                _movie_rental_create_statement.clearParameters();
                _movie_rental_create_statement.setInt(1, this.helper_max_rid() + 1);
                _movie_rental_create_statement.setInt(2, cid);
                _movie_rental_create_statement.setInt(3, mid);
                _movie_rental_create_statement.setDate(4, sqldate);
                _movie_rental_create_statement.executeUpdate();
                _commit_transaction_statement.execute();
            }
        }
    }
    
    public void transaction_return(int cid, int mid) throws Exception {
        /* return the movie mid by the customer cid */
        // first check if the customer is renting the movie
        _begin_transaction_read_write_statement.execute();
        _available_mid_statement.clearParameters();
        _available_mid_statement.setInt(1, mid);
        ResultSet available_set = _available_mid_statement.executeQuery();
        int availability = 1;
        while(available_set.next()) {
            if (available_set.getInt(3) == 1) {
                availability = 3;
                break;
            }
            else {
                if (cid == available_set.getInt(2)) {
                    availability = 2;
                    break;
                } else {
                    availability = 1;
                    break;
                }
            }
        }
        if(availability == 2) {
            _update_customer_movies_statement.clearParameters();
            _update_customer_movies_statement.setInt(1, cid);
            _update_customer_movies_statement.executeUpdate();
            
            Calendar currenttime = Calendar.getInstance();
            Date sqldate = new Date((currenttime.getTime()).getTime());
            _update_movie_return_statement.clearParameters();
            _update_movie_return_statement.setInt(2, mid);
            _update_movie_return_statement.setDate(1, sqldate);
            _update_movie_return_statement.executeUpdate();
            
            _update_movie_status_statement.clearParameters();
            _update_movie_status_statement.setInt(1, mid);
            _update_movie_status_statement.executeUpdate();
            _commit_transaction_statement.execute();
        }
        else {
            _rollback_transaction_statement.execute();
            System.out.println("You are not currently renting this movie.");
        }
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