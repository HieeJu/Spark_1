package com.example.qttg_api.service;

import com.example.qttg_api.entity.QttgMaster;
import com.example.qttg_api.repository.QttgMasterRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class QttgService {

    private final QttgMasterRepository masterRepository;

    public Page<QttgMaster> search(String keyword, int page, int size) {
        return masterRepository.searchMasterWithDetails(keyword, PageRequest.of(page, size));
    }
}
