package org.github.tursodatabase.limbo;

/**
 * Represents a database cursor.
 */
public class Cursor {
    private long cursorPtr;

    public Cursor(long cursorPtr) {
        this.cursorPtr = cursorPtr;
    }

    // TODO: support parameters
    public Cursor execute(String sql) {
         var result = execute(cursorPtr, sql);
        System.out.println("resut: " + result);
        return this;
    }

    private static native int execute(long cursorPtr, String sql);

    public Object fetchOne() throws Exception {
        Object result = fetchOne(cursorPtr);
        return processSingleResult(result);
    }

    private static native Object fetchOne(long cursorPtr);

    public Object fetchAll() throws Exception {
        Object result = fetchAll(cursorPtr);
        return processArrayResult(result);
    }

    private static native Object fetchAll(long cursorPtr);

    private Object processSingleResult(Object result) throws Exception {
        if (result instanceof Object[]) {
            System.out.println("The result is of type: Object[]");
            for (Object element : (Object[]) result) {
                printElementType(element);
            }
            return result;
        } else {
            printElementType(result);
            return result;
        }
    }

    private Object processArrayResult(Object result) throws Exception {
        if (result instanceof Object[][]) {
            System.out.println("The result is of type: Object[][]");
            Object[][] array = (Object[][]) result;
            for (Object[] row : array) {
                for (Object element : row) {
                    printElementType(element);
                }
            }
            return array;
        } else {
            throw new Exception("result should be of type Object[][]. Maybe internal logic has error.");
        }
    }

    private void printElementType(Object element) {
        if (element instanceof String) {
            System.out.println("String: " + element);
        } else if (element instanceof Integer) {
            System.out.println("Integer: " + element);
        } else if (element instanceof Double) {
            System.out.println("Double: " + element);
        } else if (element instanceof Boolean) {
            System.out.println("Boolean: " + element);
        } else if (element instanceof Long) {
            System.out.println("Long: " + element);
        } else if (element instanceof byte[]) {
            System.out.print("byte[]: ");
            for (byte b : (byte[]) element) {
                System.out.print(b + " ");
            }
            System.out.println();
        } else {
            System.out.println("Unknown type: " + element);
        }
    }
}
