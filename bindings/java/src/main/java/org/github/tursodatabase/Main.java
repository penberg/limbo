package org.github.tursodatabase;

import org.github.tursodatabase.limbo.Connection;
import org.github.tursodatabase.limbo.Cursor;
import org.github.tursodatabase.limbo.Limbo;

/**
 * TODO: Remove Main class. We can use test code to verify behaviors.
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Limbo limbo = Limbo.create();
        Connection connection = limbo.getConnection("database.db");

        Cursor cursor = connection.cursor();
        cursor.execute("SELECT * FROM example_table;");
        System.out.println("result: " + cursor.fetchOne());
    }
}
