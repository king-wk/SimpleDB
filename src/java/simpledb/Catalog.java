package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {
    //好几个方法需要利用key-value的形式，用hashmap应该是（我能想到的）最方便的吧
    //建立id到file的一一映射
    private HashMap<Integer, DbFile> idfile;
    //建立id到表名的一一映射
    private HashMap<Integer, String> idname;
    //建立id到表的主键的一一映射
    private HashMap<Integer, String> idkey;
    //建立表名到id的一一映射
    private HashMap<String, Integer> nameid;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        idfile = new HashMap<>();
        idname = new HashMap<>();
        idkey = new HashMap<>();
        nameid = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        if (name == null || pkeyField == null) {
            throw new IllegalArgumentException();
        }
        int tableid = file.getId();
        if (nameid.containsKey(tableid)) {//判断file对应的表名是否已经存在
            idfile.replace(tableid, file);//如果表名已经存在，用最新的表代替之前的表
        } else {
            idfile.put(tableid, file);
            idname.put(tableid, name);
            idkey.put(tableid, pkeyField);
            nameid.put(name, tableid);
        }
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null || (!nameid.containsKey(name))) {
            throw new NoSuchElementException();
        }
        return nameid.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!idfile.containsKey(tableid)) {//判断tableid对应的table是否存在
            throw new NoSuchElementException();
        }
        return idfile.get(tableid).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        if (!idfile.containsKey(tableid)) {//判断tableid对应的table是否存在
            throw new NoSuchElementException();
        }
        return idfile.get(tableid);
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        if (!idkey.containsKey(tableid)) {//判断tableid对应的table是否存在
            throw new NoSuchElementException();
        }
        return idkey.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return idname.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        if (!idname.containsKey(id)) {//判断id对应的name是否存在
            throw new NoSuchElementException();
        }
        return idname.get(id);
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
        //所有的hashmap都clear就行了吧
        idfile.clear();
        idname.clear();
        idkey.clear();
        nameid.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

