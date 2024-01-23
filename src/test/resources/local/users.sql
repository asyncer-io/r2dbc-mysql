CREATE TEMPORARY TABLE `users`
(
    `id`   VARCHAR(120) NOT NULL PRIMARY KEY,
    `name` VARCHAR(120) NOT NULL,
    `age`  BIGINT       NULL,
    `bio`  TEXT         NULL
)
