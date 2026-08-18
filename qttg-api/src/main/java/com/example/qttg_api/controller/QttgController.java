package com.example.qttg_api.controller;

import com.example.qttg_api.entity.QttgMaster;
import com.example.qttg_api.service.QttgService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/qttg")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")
public class QttgController {

    private final QttgService qttgService;

    @GetMapping("/search")
    public Page<QttgMaster> search(
            @RequestParam(required = false, defaultValue = "") String keyword,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return qttgService.search(keyword, page, size);
    }
}
