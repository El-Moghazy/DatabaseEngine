
public class DBApp {



    public DBApp()
    {

    }
}

class DBEngineException extends Exception {

    /**
     * Any errors related to the DB engine can be detected using these exceptions
     */
    private static final long serialVersionUID = 1L;

    public DBEngineException(String string) { super(string); }

}