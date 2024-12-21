package com.tsymbalt.peopledb.repository;

import com.tsymbalt.peopledb.annotation.SQL;
import com.tsymbalt.peopledb.model.Address;
import com.tsymbalt.peopledb.model.CrudOperation;
import com.tsymbalt.peopledb.model.Person;
import com.tsymbalt.peopledb.model.Region;

import java.math.BigDecimal;
import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;


public class PeopleRepository extends CRUDRepository<Person> {

    private AddressRepository addressRepository = null;
    public static final String INSERT_PERSON_SQL = """
            INSERT INTO PEOPLE 
            (FIRST_NAME, LAST_NAME, DOB, SALARY, EMAIL, HOME_ADDRESS, BIZ_ADDRESS ) VALUES(?, ?, ?, ?, ?, ?, ?)""";

    public static final String FIND_BY_ID_SQL = """
    SELECT 
    P.ID, P.FIRST_NAME, P.LAST_NAME, P.DOB, P.SALARY, P.HOME_ADDRESS,
    HOME.ID AS HOME_ID, HOME.STREET_ADDRESS AS HOME_STREET_ADDRESS, HOME.ADDRESS2 AS HOME_STREET_ADDRESS, HOME.CITY AS HOME_STREET_ADDRESS, HOME.STATE AS HOME_STREET_ADDRESS, HOME.POSTCODE AS HOME_STREET_ADDRESS, HOME.COUNTY AS HOME_STREET_ADDRESS, HOME.REGION AS HOME_STREET_ADDRESS, HOME.COUNTRY AS HOME_STREET_ADDRESS,
    BIZ.ID AS BIZ_ID, BIZ.STREET_ADDRESS AS BIZ_STREET_ADDRESS, BIZ.ADDRESS2 AS BIZ_STREET_ADDRESS, BIZ.CITY AS BIZ_STREET_ADDRESS, BIZ.STATE AS BIZ_STREET_ADDRESS, BIZ.POSTCODE AS BIZ_STREET_ADDRESS, BIZ.COUNTY AS BIZ_STREET_ADDRESS, BIZ.REGION AS BIZ_STREET_ADDRESS, BIZ.COUNTRY AS BIZ_STREET_ADDRESS,
    FROM PEOPLE AS P 
    LEFT OUTER JOIN ADDRESSES AS HOME ON P.HOME_ADDRESS = HOME.ID
    LEFT OUTER JOIN ADDRESSES AS BIZ ON P.BIZ_ADDRESS = BIZ.ID
    WHERE P.ID=?""";
    public static final String FIND_ALL_SQL = "SELECT ID, FIRST_NAME, LAST_NAME, DOB, SALARY FROM PEOPLE";
    public static final String SELECT_COUNT_SQL = "SELECT COUNT(*) FROM PEOPLE";
    public static final String DELETE_SQL = "DELETE FROM PEOPLE WHERE ID=?";
    public static final String DELETE_IN_SQL = "DELETE FROM PEOPLE WHERE ID IN (:ids)";
    public static final String UPDATE_SQL = "UPDATE PEOPLE SET FIRST_NAME=?, LAST_NAME=?, DOB=? WHERE ID=?";

    public PeopleRepository(Connection connection) {
        super(connection);
        addressRepository = new AddressRepository(connection);
    }
    @Override
    @SQL(value = INSERT_PERSON_SQL, operationType = CrudOperation.UPDATE)
    void mapForSave(Person entity, PreparedStatement ps) throws SQLException {
        Address savedAddress = null;

        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
        ps.setString(5, entity.getEmail());
        associateAddressWithPerson(ps, entity.getHomeAddress(), 6);
        associateAddressWithPerson(ps, entity.getHomeAddress(), 7);
    }

    private void associateAddressWithPerson(PreparedStatement ps, Optional<Address> address, int parameterIndex) throws SQLException {
        Address savedAddress;
        if (address.isPresent()) {
            savedAddress = addressRepository.save(address.get());
            ps.setLong(parameterIndex, savedAddress.id());
        } else {
            ps.setObject(parameterIndex, null);
        }
    }

    @Override
    @SQL(value = UPDATE_SQL, operationType = CrudOperation.SAVE)
    void mapForUpdate(Person entity, PreparedStatement ps) throws SQLException {
        ps.setString(1, entity.getFirstName());
        ps.setString(2, entity.getLastName());
        ps.setTimestamp(3, convertDobToTimestamp(entity.getDob()));
        ps.setBigDecimal(4, entity.getSalary());
    }

    @Override
    protected String getFindByIdSQL() {
        return FIND_BY_ID_SQL;
    }

    @Override
    @SQL(value = FIND_BY_ID_SQL, operationType = CrudOperation.FIND_BY_ID)
    @SQL(value = FIND_ALL_SQL, operationType = CrudOperation.FIND_ALL)
    @SQL(value = SELECT_COUNT_SQL, operationType = CrudOperation.COUNT)
    @SQL(value = DELETE_SQL, operationType = CrudOperation.DELETE_ONE)
    Person extractEntityFromResultSet(ResultSet rs) throws SQLException {
        long personId = rs.getLong("ID");
        String firstName = rs.getString("FIRST_NAME");
        String lastName = rs.getString("LAST_NAME");
        ZonedDateTime dob = ZonedDateTime.of(rs.getTimestamp("DOB").toLocalDateTime(), ZoneId.of("+0"));
        BigDecimal salary = rs.getBigDecimal("SALARY");
        long homeAddressId = rs.getLong("HOME_ADDRESS");

        Address homeAddress = extractAddress(rs, "HOME_");
        Address bizAddress = extractAddress(rs, "BIZ_");

        Person person = new Person(personId, firstName, lastName, dob, salary);
        person.setHomeAddress(homeAddress);
        person.setBusinessAddress(bizAddress);
        return person;
    }

    private Address extractAddress(ResultSet rs, String aliasPrefix) throws SQLException {
        Long addrId = getValueByAlias(aliasPrefix + "ID", rs, Long.class);
        if (addrId == null) return null;
        rs.getMetaData().getColumnLabel(1);
        String streetAddress = getValueByAlias(aliasPrefix + "STREET_ADDRESS", rs, String.class);
        String address2 =getValueByAlias(aliasPrefix + "ADDRESS2", rs, String.class);
        String city = getValueByAlias(aliasPrefix + "CITY", rs, String.class);
        String state = getValueByAlias(aliasPrefix + "STATE", rs, String.class);
        String postcode = getValueByAlias(aliasPrefix + "POSTCODE", rs, String.class);
        String county = getValueByAlias(aliasPrefix + "COUNTY", rs, String.class);
        Region region = Region.valueOf(getValueByAlias(aliasPrefix + "REGION", rs, String.class).toUpperCase());
        String country = getValueByAlias(aliasPrefix + "COUNTRY", rs, String.class);
        Address address = new Address(addrId, streetAddress, address2, city, state, postcode, country, county, region);
        return address;
    }

    private <T> T getValueByAlias(String alias, ResultSet rs, Class<T> clazz) throws SQLException {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int colIdx=1; colIdx <= columnCount; colIdx++) {
            if (alias.equals(rs.getMetaData().getColumnLabel(colIdx))) {
                return (T) rs.getObject(colIdx);
            }
        }
        throw new SQLException(String.format("Column not found for alias: '%s'", alias));
    }



    /*public void delete(Person...people) { // Person[] people
        for (Person person : people) {
            delete(person);
        }
    }
     */


    private Timestamp convertDobToTimestamp(ZonedDateTime dob) {
        return Timestamp.valueOf(dob.withZoneSameInstant(ZoneId.of("+0")).toLocalDateTime());
    }
}
