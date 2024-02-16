CREATE TEMPORARY TABLE `stations`
(
    `id`         INT          NOT NULL PRIMARY KEY,
    `name`       VARCHAR(120) NOT NULL,
    `created_at` DATETIME     NOT NULL
)
