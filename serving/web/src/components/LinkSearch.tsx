import { useEffect, useMemo, useRef, useState } from 'react'

interface Option { id: string; label: string }

/** Ô tìm đoạn đường: gõ để lọc theo tên/quận, Enter hoặc click để chọn. */
export function LinkSearch({ options, selected, onSelect }: {
  options: Option[]
  selected?: string
  onSelect: (id?: string) => void
}) {
  const [text, setText] = useState('')
  const [open, setOpen] = useState(false)
  const [active, setActive] = useState(0)
  const box = useRef<HTMLDivElement>(null)

  // Đồng bộ khi chọn từ bản đồ / danh sách khác
  const selLabel = options.find(o => o.id === selected)?.label
  useEffect(() => { setText(selLabel ?? '') }, [selLabel])

  useEffect(() => {
    const close = (e: MouseEvent) => { if (!box.current?.contains(e.target as Node)) setOpen(false) }
    document.addEventListener('mousedown', close)
    return () => document.removeEventListener('mousedown', close)
  }, [])

  const matches = useMemo(() => {
    const q = text.trim().toLowerCase()
    if (!q || q === selLabel?.toLowerCase()) return options
    return options.filter(o => o.label.toLowerCase().includes(q) || o.id.includes(q))
  }, [text, options, selLabel])

  const pick = (o: Option) => { onSelect(o.id); setText(o.label); setOpen(false) }

  return (
    <div className="search" ref={box}>
      <span className="search-icon">⌕</span>
      <input
        value={text}
        placeholder="Tìm đoạn đường…"
        onFocus={() => { setOpen(true); setActive(0) }}
        onChange={e => { setText(e.target.value); setOpen(true); setActive(0) }}
        onKeyDown={e => {
          if (e.key === 'ArrowDown') { e.preventDefault(); setActive(a => Math.min(a + 1, matches.length - 1)) }
          else if (e.key === 'ArrowUp') { e.preventDefault(); setActive(a => Math.max(a - 1, 0)) }
          else if (e.key === 'Enter' && matches[active]) pick(matches[active])
          else if (e.key === 'Escape') setOpen(false)
        }}
      />
      {(text || selected) && (
        <button className="search-clear" aria-label="Bỏ chọn" onClick={() => { setText(''); onSelect(undefined) }}>×</button>
      )}
      {open && (
        <ul className="search-list">
          {matches.length === 0 && <li className="empty">Không tìm thấy</li>}
          {matches.slice(0, 60).map((o, i) => (
            <li
              key={o.id}
              className={`${i === active ? 'active' : ''} ${o.id === selected ? 'sel' : ''}`}
              onMouseEnter={() => setActive(i)}
              onMouseDown={e => { e.preventDefault(); pick(o) }}
            >
              {o.label}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
