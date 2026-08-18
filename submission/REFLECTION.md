Trong "Top 5 Lakehouse Anti-Patterns", team em de vuong nhat la **small files va maintenance bi bo quen**. Ly do la workload observability/agent trace thuong ghi theo micro-batch nho, nhieu tenant, nhieu partition theo ngay/model/version. Neu chi tap trung ingest cho nhanh ma khong dat lich compact, clustering, vacuum/expire snapshot va orphan cleanup, storage se phinh ra rat nhanh trong khi query dashboard lai cham dan.

Lab lam diem nay rat ro: NB2 cho thay cung mot predicate nhung file layout tot co the prune manh hon nhieu; NB6 con nguy hiem hon vi `VACUUM` khong tu don orphan chua tung commit, va Iceberg `expire_snapshots` chi lam metadata snapshot khong tu xoa het file vat ly. Nghia la team co the tuong minh da "don dep" nhung hoa don object storage van tang.

De tranh anti-pattern nay, em se coi maintenance la production job bat buoc: co SLO, metric before/after, alert khi file count/metadata ratio vuot nguong, va rollback bang time travel neu job toi uu lam sai.
