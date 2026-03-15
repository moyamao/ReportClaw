CREATE TABLE annual_report_announcements (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    announcement_id VARCHAR(64) NOT NULL,
    sec_code VARCHAR(16) NOT NULL,
    sec_name VARCHAR(64),
    org_id VARCHAR(64),
    market VARCHAR(16),
    report_year INT,
    announcement_title VARCHAR(255) NOT NULL,
    announcement_time DATETIME,
    adjunct_url VARCHAR(500),
    pdf_url VARCHAR(500),
    source_json JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_announcement_id (announcement_id),
    KEY idx_code_year (sec_code, report_year),
    KEY idx_announcement_time (announcement_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



CREATE TABLE annual_report_files (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    announcement_id VARCHAR(64) NOT NULL,
    local_path VARCHAR(500) NOT NULL,
    file_name VARCHAR(255),
    file_size BIGINT,
    file_md5 CHAR(32),
    page_count INT,
    is_text_pdf TINYINT DEFAULT 1,
    download_status VARCHAR(32) DEFAULT 'pending',
    parse_status VARCHAR(32) DEFAULT 'pending',
    parse_error TEXT,
    downloaded_at DATETIME NULL,
    parsed_at DATETIME NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_announcement_file (announcement_id),
    KEY idx_parse_status (parse_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE annual_report_sections (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    announcement_id VARCHAR(64) NOT NULL,
    parent_id BIGINT NULL,
    level_num INT NOT NULL,
    section_title VARCHAR(255) NOT NULL,
    normalized_title VARCHAR(255),
    title_no VARCHAR(64),
    path VARCHAR(1000),
    start_page INT,
    end_page INT,
    start_line_no INT,
    end_line_no INT,
    title_score DECIMAL(8,4) NULL,
    match_method VARCHAR(64),
    content_preview VARCHAR(1000),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_announcement_level (announcement_id, level_num),
    KEY idx_announcement_pages (announcement_id, start_page, end_page),
    KEY idx_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE annual_report_target_sections (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    announcement_id VARCHAR(64) NOT NULL,
    target_key VARCHAR(64) NOT NULL,
    matched_section_id BIGINT NULL,
    matched_title VARCHAR(255),
    matched_path VARCHAR(1000),
    start_page INT,
    end_page INT,
    extract_confidence DECIMAL(8,4),
    content LONGTEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_announcement_target (announcement_id, target_key),
    KEY idx_target_key (target_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE annual_report_parse_logs (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    announcement_id VARCHAR(64) NOT NULL,
    stage VARCHAR(64) NOT NULL,
    level_name VARCHAR(16) DEFAULT 'INFO',
    message TEXT,
    extra_json JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    KEY idx_announcement_stage (announcement_id, stage),
    KEY idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



