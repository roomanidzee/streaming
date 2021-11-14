package com.romanidze.processing.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.sql.Timestamp;
import java.util.Objects;

@JsonSerialize
public class PersonModel {

  private Integer id;

  @JsonProperty("firstname")
  private String firstName;

  @JsonProperty("lastname")
  private String lastName;

  private String email;

  private String profession;

  private String city;

  private String country;

  @JsonProperty("random_date")
  private Timestamp randomDate;

  public PersonModel(
      Integer id,
      String firstName,
      String lastName,
      String email,
      String profession,
      String city,
      String country,
      Timestamp randomDate) {
    this.id = id;
    this.firstName = firstName;
    this.lastName = lastName;
    this.email = email;
    this.profession = profession;
    this.city = city;
    this.country = country;
    this.randomDate = randomDate;
  }

  public PersonModel() {}

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getProfession() {
    return profession;
  }

  public void setProfession(String profession) {
    this.profession = profession;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }

  public Timestamp getRandomDate() {
    return randomDate;
  }

  public void setRandomDate(Timestamp randomDate) {
    this.randomDate = randomDate;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PersonModel{");
    sb.append("id=").append(id);
    sb.append(", firstName='").append(firstName).append('\'');
    sb.append(", lastName='").append(lastName).append('\'');
    sb.append(", email='").append(email).append('\'');
    sb.append(", profession='").append(profession).append('\'');
    sb.append(", city='").append(city).append('\'');
    sb.append(", country='").append(country).append('\'');
    sb.append(", randomDate=").append(randomDate);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PersonModel that = (PersonModel) o;
    return getId().equals(that.getId())
        && getFirstName().equals(that.getFirstName())
        && getLastName().equals(that.getLastName())
        && getEmail().equals(that.getEmail())
        && getProfession().equals(that.getProfession())
        && getCity().equals(that.getCity())
        && getCountry().equals(that.getCountry())
        && getRandomDate().equals(that.getRandomDate());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getId(),
        getFirstName(),
        getLastName(),
        getEmail(),
        getProfession(),
        getCity(),
        getCountry(),
        getRandomDate());
  }
}
