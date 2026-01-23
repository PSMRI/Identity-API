/*
* AMRIT – Accessible Medical Records via Integrated Technology 
* Integrated EHR (Electronic Health Records) Solution 
*
* Copyright (C) "Piramal Swasthya Management and Research Institute" 
*
* This file is part of AMRIT.
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this program.  If not, see https://www.gnu.org/licenses/.
*/
package com.iemr.common.identity.repo;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.iemr.common.identity.domain.MBeneficiarydetail;

import jakarta.transaction.Transactional;

@Repository
public interface BenDetailRepo extends CrudRepository<MBeneficiarydetail, BigInteger>, BenDetailRepoCustom {
	List<MBeneficiarydetail> findByBeneficiaryDetailsIdOrderByBeneficiaryDetailsIdAsc(BigInteger beneficiaryDetailsId);

	List<MBeneficiarydetail> findByCreatedDateBetweenOrderByBeneficiaryDetailsIdAsc(Timestamp fromDate,
			Timestamp toDate);

	List<MBeneficiarydetail> findByFirstNameAndLastNameOrderByBeneficiaryDetailsIdAsc(String fName, String mName);


	@Query("select d from MBeneficiarydetail d " + "where d.mBeneficiarymapping.benRegId = :benRegId")
	MBeneficiarydetail findByBenRegId(BigInteger benRegId);


	@Query("select d from MBeneficiarydetail d where d.firstName = :fName and d.middleName = :mName and "
			+ "d.lastName = :lName Order By d.beneficiaryDetailsId Asc")
	List<MBeneficiarydetail> findByName(String fName, String mName, String lName);

	@Query("select d from MBeneficiarydetail d where d.firstName = :fName and d.middleName = :mName and "
			+ "d.lastName = :lName and d.spouseName = :spouseName Order By d.beneficiaryDetailsId Asc")
	List<MBeneficiarydetail> findByNameAndSpouseName(String fName, String mName, String lName, String spouseName);

	@Query("select d from MBeneficiarydetail d where d.firstName = :fName and d.middleName = :mName and "
			+ "d.lastName = :lName and d.fatherName = :fatherName and d.dob = :dob  Order By d.beneficiaryDetailsId Asc")
	List<MBeneficiarydetail> findByNameAndFatherNameAndDob(String fName, String mName, String lName, String fatherName,
			Timestamp dob);

	@Query("select d from MBeneficiarydetail d where d.firstName = :fName and d.middleName = :mName and "
			+ "d.lastName = :lName and d.dob = :dob Order By d.beneficiaryDetailsId Asc")
	List<MBeneficiarydetail> findByNameAndDob(String fName, String mName, String lName, Timestamp dob);

	@Query("select d from MBeneficiarydetail d where d.firstName = :fName and d.middleName = :mName and "
			+ "d.lastName = :lName and d.spouseName = :spouseName and d.dob = :dob Order By d.beneficiaryDetailsId Asc")
	List<MBeneficiarydetail> findByNameAndSpouseNameAndDob(String fName, String mName, String lName, String spouseName,
			Timestamp dob);

	@Query("select d from MBeneficiarydetail d where d.firstName = :fName and d.middleName = :mName and "
			+ "d.lastName = :lName and d.fatherName = :fatherName Order By d.beneficiaryDetailsId Asc")
	List<MBeneficiarydetail> findByNameAndFatherName(String fName, String mName, String lName, String fatherName);

	@Query("select new MBeneficiarydetail(d.beneficiaryDetailsId, d.firstName, d.lastName, d.middleName, "
			+ "d.fatherName, d.spouseName) "
			+ "from MBeneficiarydetail d where d.mBeneficiarymapping.benRegId = :benRegId")
	MBeneficiarydetail findPartialBeneficiaryDetailByBenRegId(@Param("benRegId") BigInteger benRegId);

	@Query("select new MBeneficiarydetail(d.beneficiaryDetailsId, d.firstName, d.lastName, d.middleName, "
			+ "d.fatherName, d.spouseName) "
			+ "from MBeneficiarydetail d where d.mBeneficiarymapping.benRegId in :benRegIds")
	List<MBeneficiarydetail> findPartialBeneficiaryDetailByBenRegId(@Param("benRegIds") List<BigInteger> benRegId);

	@Transactional
	@Modifying
	@Query("UPDATE MBeneficiarydetail c SET c.communityId = :communityId WHERE c.vanSerialNo = :id AND c.vanID = :vanID  ")
	Integer updateCommunity(@Param("id") BigInteger id, @Param("vanID") Integer vanID,
			@Param("communityId") Integer communityId);

	@Transactional
	@Modifying
	@Query("UPDATE MBeneficiarydetail c SET c.educationId = :educationId WHERE c.vanSerialNo = :id AND c.vanID = :vanID ")
	Integer updateEducation(@Param("id") BigInteger id, @Param("vanID") Integer vanID,
			@Param("educationId") Integer educationId);

	@Transactional
	@Modifying
	@Query(" UPDATE MBeneficiarydetail set vanSerialNo = :beneficiaryDetailsId WHERE beneficiaryDetailsId = :beneficiaryDetailsId")
	int updateVanSerialNo(@Param("beneficiaryDetailsId") BigInteger beneficiaryDetailsId);

	@Query("SELECT beneficiaryDetailsId FROM MBeneficiarydetail WHERE vanSerialNo =:vanSerialNo AND vanID =:vanID ")
	BigInteger findIdByVanSerialNoAndVanID(@Param("vanSerialNo") BigInteger vanSerialNo, @Param("vanID") Integer vanID);

	@Query("SELECT t FROM MBeneficiarydetail t WHERE t.vanSerialNo =:vanSerialNo AND t.vanID =:vanID ")
	MBeneficiarydetail findBenDetailsByVanSerialNoAndVanID(@Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") Integer vanID);

	@Query("SELECT a FROM MBeneficiarydetail a WHERE a.vanSerialNo =:vanSerialNo AND a.vanID =:vanID ")
	MBeneficiarydetail getWith_vanSerialNo_vanID(@Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") Integer vanID);

	@Query("SELECT a FROM MBeneficiarydetail a WHERE a.familyId =:familyId")
	List<MBeneficiarydetail> getFamilyDetails(@Param("familyId") String familyId);

	@Transactional
	@Modifying
	@Query("UPDATE MBeneficiarydetail c SET c.familyId = :familyId,c.headOfFamily_RelationID =:headofFamily_RelationID, "
			+ " c.headOfFamily_Relation =:headofFamily_Relation,c.other =:other "
			+ " WHERE c.vanSerialNo =:vanSerialNo AND c.vanID =:vanID ")
	Integer updateFamilyDetails(@Param("familyId") String familyId,
			@Param("headofFamily_RelationID") Integer headofFamilyRelationID,
			@Param("headofFamily_Relation") String headofFamilyRelation, @Param("other") String other,
			@Param("vanSerialNo") BigInteger vanSerialNo, @Param("vanID") Integer vanID);

	@Transactional
	@Modifying
	@Query("UPDATE MBeneficiarydetail c SET c.headOfFamily_RelationID =:headofFamily_RelationID, "
			+ " c.headOfFamily_Relation =:headofFamily_Relation,c.other =:other "
			+ " WHERE c.vanSerialNo =:vanSerialNo AND c.vanID =:vanID AND c.familyId=:familyId ")
	Integer editFamilyDetails(@Param("headofFamily_RelationID") Integer headofFamilyRelationID,
			@Param("headofFamily_Relation") String headofFamilyRelation, @Param("other") String other,
			@Param("vanSerialNo") BigInteger vanSerialNo, @Param("vanID") Integer vanID,
			@Param("familyId") String familyId);

	@Transactional
	@Modifying
	@Query("UPDATE MBeneficiarydetail c SET c.familyId = null,c.headOfFamily_RelationID = null,c.other = null, "
			+ " c.headOfFamily_Relation = null,c.modifiedBy = :modifiedBy "
			+ " WHERE c.vanSerialNo =:vanSerialNo AND c.vanID =:vanID ")
	int untagFamily(@Param("modifiedBy") String modifiedBy, @Param("vanSerialNo") BigInteger vanSerialNo,
			@Param("vanID") Integer vanID);

	@Query("SELECT b FROM MBeneficiarydetail b WHERE b.familyId =:familyid  ")
	List<MBeneficiarydetail> searchByFamilyId(@Param("familyid") String familyid);

	  /**
     * Find complete beneficiary data by IDs from Elasticsearch
     */
    @Query(value = "SELECT " +
        "m.BenRegId, " +                                    // 0
        "brm.beneficiaryID, " +                               // 1
        "d.FirstName, " +                                   // 2
        "d.LastName, " +                                    // 3
        "d.GenderID, " +                                    // 4
        "g.GenderName, " +                                  // 5
        "d.DOB, " +                                         // 6
        "TIMESTAMPDIFF(YEAR, d.DOB, CURDATE()) as Age, " + // 7
        "d.FatherName, " +                                  // 8
        "d.SpouseName, " +                                  // 9
        "d.IsHIVPositive, " +                                    // 10
        "m.CreatedBy, " +                                   // 11
        "m.CreatedDate, " +                                 // 12
        "UNIX_TIMESTAMP(m.LastModDate) * 1000, " +          // 13
        "m.BenAccountID, " +                                // 14
        "addr.CurrStateId, " +                              // 15
        "addr.CurrState, " +                                // 16
        "addr.CurrDistrictId, " +                           // 17
        "addr.CurrDistrict, " +                             // 18
        "addr.CurrSubDistrictId, " +                        // 19
        "addr.CurrSubDistrict, " +                          // 20
        "addr.CurrPinCode, " +                              // 21
        "addr.CurrServicePointId, " +                       // 22
        "addr.CurrServicePoint, " +                         // 23
        "addr.ParkingPlaceID, " +                           // 24
        "contact.PreferredPhoneNum " +                      // 25
        "addr.CurrVillageId, " +                            // 26 
        "addr.CurrVillage " +                               // 27 
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarydetails d ON m.BenDetailsId = d.BeneficiaryDetailsID " +
		"LEFT JOIN m_beneficiaryregidmapping brm ON brm.BenRegId = m.BenRegId " +
        "LEFT JOIN db_iemr.m_gender g ON d.GenderID = g.GenderID " +
        "LEFT JOIN i_beneficiaryaddress addr ON m.BenAddressId = addr.BenAddressID " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId IN (:ids) AND m.Deleted = false", 
        nativeQuery = true)
    List<Object[]> findCompleteDataByIds(@Param("ids") List<Long> ids);
    
    /**
     * Direct search in database (fallback)
     */
    @Query(value = "SELECT " +
        "m.BenRegId, " +
        "brm.beneficiaryID, " +
        "d.FirstName, " +
        "d.LastName, " +
        "d.GenderID, " +
        "g.GenderName, " +
        "d.DOB, " +
        "TIMESTAMPDIFF(YEAR, d.DOB, CURDATE()) as Age, " +
        "d.FatherName, " +
        "d.SpouseName, " +
        "d.IsHIVPositive, " +
        "m.CreatedBy, " +
        "m.CreatedDate, " +
        "UNIX_TIMESTAMP(m.LastModDate) * 1000, " +
        "m.BenAccountID, " +
        "addr.CurrStateId, " +
        "addr.CurrState, " +
        "addr.CurrDistrictId, " +
        "addr.CurrDistrict, " +
        "addr.CurrSubDistrictId, " +
        "addr.CurrSubDistrict, " +
        "addr.CurrPinCode, " +
        "addr.CurrServicePointId, " +
        "addr.CurrServicePoint, " +
        "addr.ParkingPlaceID, " +
        "contact.PreferredPhoneNum " +
        "addr.CurrVillageId, " +                           
        "addr.CurrVillage " +                               
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarydetails d ON m.BenDetailsId = d.BeneficiaryDetailsID " +
        "LEFT JOIN db_iemr.m_gender g ON d.GenderID = g.GenderID " +
		"LEFT JOIN m_beneficiaryregidmapping brm ON brm.BenRegId = m.BenRegId " +
        "LEFT JOIN i_beneficiaryaddress addr ON m.BenAddressId = addr.BenAddressID " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE (d.FirstName LIKE CONCAT('%', :query, '%') " +
        "   OR d.LastName LIKE CONCAT('%', :query, '%') " +
        "   OR d.FatherName LIKE CONCAT('%', :query, '%') " +
        "   OR d.BeneficiaryRegID = :query " +
        "   OR contact.PreferredPhoneNum = :query " +
        "   OR contact.PhoneNum1 = :query " +
        "   OR contact.PhoneNum2 = :query " +
        "   OR contact.PhoneNum3 = :query " +
        "   OR contact.PhoneNum4 = :query " +
        "   OR contact.PhoneNum5 = :query) " +
        "AND m.Deleted = false " +
        "LIMIT 20", 
        nativeQuery = true)
    List<Object[]> searchBeneficiaries(@Param("query") String query);
    
    /**
     * Get all phone numbers for a beneficiary
     */
    @Query(value = "SELECT " +
        "contact.PreferredPhoneNum as phoneNo, " +
        "'Preferred' as phoneType, " +
        "1 as priority " +
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId = :beneficiaryId AND contact.PreferredPhoneNum IS NOT NULL " +
        "UNION ALL " +
        "SELECT contact.PhoneNum1, contact.PhoneTyp1, 2 " +
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId = :beneficiaryId AND contact.PhoneNum1 IS NOT NULL " +
        "UNION ALL " +
        "SELECT contact.PhoneNum2, contact.PhoneTyp2, 3 " +
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId = :beneficiaryId AND contact.PhoneNum2 IS NOT NULL " +
        "UNION ALL " +
        "SELECT contact.PhoneNum3, contact.PhoneTyp3, 4 " +
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId = :beneficiaryId AND contact.PhoneNum3 IS NOT NULL " +
        "UNION ALL " +
        "SELECT contact.PhoneNum4, contact.PhoneTyp4, 5 " +
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId = :beneficiaryId AND contact.PhoneNum4 IS NOT NULL " +
        "UNION ALL " +
        "SELECT contact.PhoneNum5, contact.PhoneTyp5, 6 " +
        "FROM i_beneficiarymapping m " +
        "LEFT JOIN i_beneficiarycontacts contact ON m.BenContactsId = contact.BenContactsID " +
        "WHERE m.BenRegId = :beneficiaryId AND contact.PhoneNum5 IS NOT NULL " +
        "ORDER BY priority", 
        nativeQuery = true)
    List<Object[]> findPhoneNumbersByBeneficiaryId(@Param("beneficiaryId") Long beneficiaryId);
    
// Advance Search ES
@Query(value =
    "SELECT DISTINCT " +
    "m.BenRegId, " +                                 // 0
    "brm.beneficiaryID, " +                         // 1
    "d.FirstName, " +                               // 2
    "d.LastName, " +                                // 3
    "d.GenderID, " +                                // 4
    "g.GenderName, " +                              // 5
    "d.DOB, " +                                     // 6
    "TIMESTAMPDIFF(YEAR, d.DOB, CURDATE()) AS Age, "+// 7
    "d.FatherName, " +                              // 8
    "d.SpouseName, " +                              // 9
    "d.IsHIVPositive, " +                           // 10
    "m.CreatedBy, " +                               // 11
    "m.CreatedDate, " +                             // 12
    "UNIX_TIMESTAMP(m.LastModDate) * 1000, " +      // 13
    "m.BenAccountID, " +                            // 14
    "addr.CurrStateId, " +                          // 15
    "addr.CurrState, " +                            // 16
    "addr.CurrDistrictId, " +                       // 17
    "addr.CurrDistrict, " +                         // 18
    "addr.CurrSubDistrictId, " +                    // 19
    "addr.CurrSubDistrict, " +                      // 20
    "addr.CurrPinCode, " +                          // 21
    "addr.CurrServicePointId, " +                   // 22
    "addr.CurrServicePoint, " +                     // 23
    "addr.ParkingPlaceID, " +                       // 24
    "contact.PreferredPhoneNum " +                  // 25
    "addr.CurrVillageId, " +                            
    "addr.CurrVillage " +                               
    "FROM i_beneficiarymapping m " +
    "LEFT JOIN i_beneficiarydetails d " +
    "       ON m.BenDetailsId = d.BeneficiaryDetailsID " +
    "LEFT JOIN db_iemr.m_gender g " +
    "       ON d.GenderID = g.GenderID " +
    "LEFT JOIN i_beneficiaryaddress addr " +
    "       ON m.BenAddressId = addr.BenAddressID " +
    "LEFT JOIN i_beneficiarycontacts contact " +
    "       ON m.BenContactsId = contact.BenContactsID " +
	"LEFT JOIN m_beneficiaryregidmapping brm ON brm.BenRegId = m.BenRegId " +
    "WHERE m.Deleted = false " +

    "AND (:firstName IS NULL OR d.FirstName LIKE CONCAT('%', :firstName, '%')) " +
    "AND (:lastName IS NULL OR d.LastName LIKE CONCAT('%', :lastName, '%')) " +
    "AND (:genderId IS NULL OR d.GenderID = :genderId) " +
    "AND (:dob IS NULL OR DATE(d.DOB) = DATE(:dob)) " +

    "AND (:stateId IS NULL OR addr.CurrStateId = :stateId) " +
    "AND (:districtId IS NULL OR addr.CurrDistrictId = :districtId) " +
    "AND (:blockId IS NULL OR addr.CurrSubDistrictId = :blockId) " +

    "AND (:fatherName IS NULL OR d.FatherName LIKE CONCAT('%', :fatherName, '%')) " +
    "AND (:spouseName IS NULL OR d.SpouseName LIKE CONCAT('%', :spouseName, '%')) " +

    "AND (:phoneNumber IS NULL OR " +
    "     contact.PreferredPhoneNum LIKE CONCAT('%', :phoneNumber, '%') OR " +
    "     contact.PhoneNum1 LIKE CONCAT('%', :phoneNumber, '%') OR " +
    "     contact.PhoneNum2 LIKE CONCAT('%', :phoneNumber, '%') OR " +
    "     contact.PhoneNum3 LIKE CONCAT('%', :phoneNumber, '%') OR " +
    "     contact.PhoneNum4 LIKE CONCAT('%', :phoneNumber, '%') OR " +
    "     contact.PhoneNum5 LIKE CONCAT('%', :phoneNumber, '%')) " +

    "AND (:beneficiaryId IS NULL OR d.BeneficiaryRegID = :beneficiaryId) " +

    "ORDER BY m.CreatedDate DESC " +
    "LIMIT 100",
    nativeQuery = true)
List<Object[]> advancedSearchBeneficiaries(
    @Param("firstName") String firstName,
    @Param("lastName") String lastName,
    @Param("genderId") Integer genderId,
    @Param("dob") Date dob,
    @Param("stateId") Integer stateId,
    @Param("districtId") Integer districtId,
    @Param("blockId") Integer blockId,
    @Param("fatherName") String fatherName,
    @Param("spouseName") String spouseName,
    @Param("phoneNumber") String phoneNumber,
    @Param("beneficiaryId") String beneficiaryId
);


}
