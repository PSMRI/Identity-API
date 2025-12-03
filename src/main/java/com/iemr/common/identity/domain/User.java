package com.iemr.common.identity.domain;

import java.sql.Timestamp;
import java.time.LocalDate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.gson.annotations.Expose;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
@Entity
@Table(name = "m_User",schema = "db_iemr")
@Data
public class User {
	@Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Expose
    @Column(name="UserID")
    private Integer userID;
    @Expose
    @Column(name="TitleID")
    private Integer titleID;
    @Expose
    @Column(name="FirstName")
    private String firstName;
    @Expose
    @Column(name="MiddleName")
    private String middleName;
    @Expose
    @Column(name="LastName")
    private String lastName;
    @Expose
    @Column(name="GenderID")
    private Short genderID;

    @Expose
    @Column(name="MaritalStatusID")
    private Integer maritalStatusID;
    @Expose
    @Column(name="DesignationID")
    private Integer designationID;

    @Column(name="AadhaarNo")
    private String aadhaarNo;
    @Column(name="PAN")
    private String pAN;
    @Expose
    @Column(name="DOB")
    private LocalDate dOB;
    @Expose
    @Column(name="DOJ")
    private LocalDate dOJ;
    @Expose
    @Column(name="QualificationID")
    private Integer qualificationID;
    @Expose
    @Column(name="HealthProfessionalID")
    private String healthProfessionalID;
    @Expose
    @Column(name="UserName")
    private String userName;
    @JsonIgnore
    @Column(name="Password")
    private String password;
    @Expose
    @Column(name="IsExternal")
    private Boolean isExternal;
    @Expose
    @Column(name="AgentID")
    private String agentID;
    @Expose
    @Column(name="AgentPassword")
    private String agentPassword;
    @Expose
    @Column(name="EmailID")
    private String emailID;
    @Expose
    @Column(name="StatusID")
    private Integer statusID;
    @Expose
    @Column(name="EmergencyContactPerson")
    private String emergencyContactPerson;
    @Expose
    @Column(name="EmergencyContactNo")
    private String emergencyContactNo;
    @Expose
    @Column(name="IsSupervisor")
    private Boolean isSupervisor;
    @Expose
    @Column(name="Deleted",insertable = false, updatable = true)
    private Boolean deleted;
    @Expose
    @Column(name="CreatedBy")
    private String createdBy;
    @Expose
    @Column(name="EmployeeID")
    private String employeeID;
    @Expose
    @Column(name="CreatedDate",insertable = false, updatable = false)
    private Timestamp createdDate;
    @Expose
    @Column(name="ModifiedBy")
    private String modifiedBy;
    @Expose
    @Column(name="LastModDate",insertable = false, updatable = false)
    private Timestamp lastModDate;

    @Expose
    @Column(name="Remarks")
    private String remarks;

    @Expose
    @Column(name="ContactNo")
    private String contactNo;


    @Expose
    @Column(name="IsProviderAdmin")
    private Boolean isProviderAdmin;

    @Expose
    @Column(name="ServiceProviderID")
    private Integer serviceProviderID;



    @Expose
    @Column(name = "failed_attempt", insertable = false)
    private Integer failedAttempt;
    public User() {
        // TODO Auto-generated constructor stub
    }

    public User(Integer userID, String userName) {
        // TODO Auto-generated constructor stub
        this.userID=userID;
        this.userName=userName;
    }

}
