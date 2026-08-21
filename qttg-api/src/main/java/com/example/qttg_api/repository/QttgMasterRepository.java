package com.example.qttg_api.repository;

import com.example.qttg_api.entity.QttgMaster;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface QttgMasterRepository extends JpaRepository<QttgMaster, Long> {

    @Query(value = "SELECT DISTINCT m FROM QttgMaster m " +
                   "LEFT JOIN FETCH m.details d " +
                   "WHERE :keyword IS NULL OR :keyword = '' " +
                   "OR LOWER(m.soSoBhxh) LIKE LOWER(CONCAT('%', :keyword, '%')) " +
                   "OR LOWER(d.tenDonVi) LIKE LOWER(CONCAT('%', :keyword, '%'))",
           countQuery = "SELECT COUNT(DISTINCT m) FROM QttgMaster m " +
                        "LEFT JOIN m.details d " +
                        "WHERE :keyword IS NULL OR :keyword = '' " +
                        "OR LOWER(m.soSoBhxh) LIKE LOWER(CONCAT('%', :keyword, '%')) " +
                        "OR LOWER(d.tenDonVi) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    Page<QttgMaster> searchMasterWithDetails(@Param("keyword") String keyword, Pageable pageable);
}