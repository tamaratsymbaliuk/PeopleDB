package com.tsymbalt.peopledb.repository;

import com.tsymbalt.peopledb.model.Person;

import java.sql.Connection;

public class PeopleRepository {
    private Connection connection;
    public PeopleRepository(Connection connection) {
        this.connection = connection;

    }
    public PeopleRepository() {

    }


    public Person save(Person person) {
        return person;
    }
}
