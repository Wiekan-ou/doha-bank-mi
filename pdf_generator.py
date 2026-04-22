from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import os
import sys
import json
from datetime import date as dt

pdfmetrics.registerFont(TTFont('Caladea',        '/usr/share/fonts/truetype/crosextra/Caladea-Regular.ttf'))
pdfmetrics.registerFont(TTFont('Caladea-Bold',   '/usr/share/fonts/truetype/crosextra/Caladea-Bold.ttf'))
pdfmetrics.registerFont(TTFont('Caladea-Italic', '/usr/share/fonts/truetype/crosextra/Caladea-Italic.ttf'))
pdfmetrics.registerFont(TTFont('Carlito',        '/usr/share/fonts/truetype/crosextra/Carlito-Regular.ttf'))
pdfmetrics.registerFont(TTFont('Carlito-Bold',   '/usr/share/fonts/truetype/crosextra/Carlito-Bold.ttf'))
pdfmetrics.registerFont(TTFont('Carlito-Italic', '/usr/share/fonts/truetype/crosextra/Carlito-Italic.ttf'))

W, H = landscape(A4)
M = 11 * mm
UW = W - 2 * M

BLUE    = colors.HexColor('#1a5fa8')
NAVY    = colors.HexColor('#0d2c5e')
CYAN    = colors.HexColor('#00aeef')
GOLD    = colors.HexColor('#c9a84c')
WHITE   = colors.white
OFFWHT  = colors.HexColor('#f4f8fd')
TBLHDR  = colors.HexColor('#0f3d7a')
RULE    = colors.HexColor('#c5d8ee')
RULE_DK = colors.HexColor('#7aafd4')
TEXT    = colors.HexColor('#1a2a3a')
MUTED   = colors.HexColor('#5a7a96')
UP      = colors.HexColor('#1a7a45')
DOWN    = colors.HexColor('#c0392b')
SUBT    = colors.HexColor('#9ac4e8')
WARN    = colors.HexColor('#b45309')

HDR_H = 24 * mm
FTR_H = 5.5 * mm
KPI_H = 14 * mm
SEC_H = 5.5 * mm
ROW_H = 4.6 * mm
GAP   = 2.5 * mm


def pct_col(v):
    v = str(v)
    if v.startswith('+'):
        return UP
    if v.startswith('-'):
        return DOWN
    if v == "N/A":
        return MUTED
    return MUTED


def fr(c, x, y, w, h, col):
    c.setFillColor(col)
    c.rect(x, y, w, h, fill=1, stroke=0)


def sr(c, x, y, w, h, col, lw=0.4):
    c.setStrokeColor(col)
    c.setLineWidth(lw)
    c.rect(x, y, w, h, fill=0, stroke=1)


def t(c, txt, x, y, font='Carlito', size=8, color=TEXT, align='left', maxw=None):
    txt = '' if txt is None else str(txt)
    c.setFont(font, size)
    c.setFillColor(color)
    if maxw:
        while len(txt) > 4 and c.stringWidth(txt, font, size) > maxw:
            txt = txt[:-4] + '...'
    if align == 'right':
        c.drawRightString(x, y, txt)
    elif align == 'center':
        c.drawCentredString(x, y, txt)
    else:
        c.drawString(x, y, txt)


def hl(c, x1, y, x2, col=RULE, lw=0.35):
    c.setStrokeColor(col)
    c.setLineWidth(lw)
    c.line(x1, y, x2, y)


def ml(c, txt, x, y, font, size, color, maxw, lh, maxl=3):
    txt = '' if txt is None else str(txt)
    c.setFont(font, size)
    c.setFillColor(color)
    words = txt.split()
    lines = []
    line = ''
    for w in words:
        tt = (line + ' ' + w).strip()
        if c.stringWidth(tt, font, size) <= maxw:
            line = tt
        else:
            if line:
                lines.append(line)
            line = w
            if len(lines) >= maxl:
                break
    if line and len(lines) < maxl:
        lines.append(line)
    for i, ln in enumerate(lines[:maxl]):
        c.drawString(x, y - i * lh, ln)
    return y - len(lines[:maxl]) * lh


def clean_px(px):
    if isinstance(px, (float, int)):
        if abs(px) >= 1000:
            return f"{px:,.2f}"
        if abs(px) >= 10:
            return f"{px:,.2f}"
        return f"{px:,.4f}" if abs(px) < 1 else f"{px:,.2f}"
    return str(px)


def draw_header(c, report_date, generated_display_time, page=1, total=2, report_status="ok"):
    fr(c, 0, H - HDR_H, W, HDR_H, BLUE)
    fr(c, 0, H - HDR_H, 56 * mm, HDR_H, NAVY)

    c.setStrokeColor(colors.HexColor('#2a6fc0'))
    c.setLineWidth(0.5)
    c.line(56 * mm, H - HDR_H + 3 * mm, 56 * mm, H - 3 * mm)

    c.setFillColor(WHITE)
    c.setStrokeColor(CYAN)
    c.setLineWidth(0.8)
    c.roundRect(5 * mm, H - HDR_H + 5 * mm, 12 * mm, 12 * mm, 1.5 * mm, fill=1, stroke=1)
    t(c, 'D', 11 * mm, H - HDR_H + 10 * mm, 'Caladea-Bold', 10, BLUE, 'center')
    t(c, 'بنك الدوحة', 20 * mm, H - HDR_H + 18 * mm, 'Carlito', 7, SUBT)
    t(c, 'DOHA BANK', 20 * mm, H - HDR_H + 11.5 * mm, 'Caladea-Bold', 10, WHITE)

    c.setStrokeColor(GOLD)
    c.setLineWidth(0.8)
    c.line(W / 2 - 52 * mm, H - HDR_H + 14 * mm, W / 2 - 26 * mm, H - HDR_H + 14 * mm)
    c.line(W / 2 + 26 * mm, H - HDR_H + 14 * mm, W / 2 + 52 * mm, H - HDR_H + 14 * mm)

    t(c, 'MARKET INTELLIGENCE', W / 2, H - HDR_H + 19 * mm, 'Caladea-Bold', 14, WHITE, 'center')
    t(c, report_date, W / 2, H - HDR_H + 12.5 * mm, 'Carlito', 9, GOLD, 'center')
    t(c, 'Market Snapshot  |  Currency & Fixed Income  |  Global & Qatar News',
      W / 2, H - HDR_H + 7 * mm, 'Carlito-Italic', 6.5, SUBT, 'center')

    t(c, f'Page {page} of {total}', W - M, H - HDR_H + 19 * mm, 'Carlito', 6.5, GOLD, 'right')
    t(c, f'Generated  {generated_display_time}', W - M, H - HDR_H + 14 * mm, 'Carlito', 6, SUBT, 'right')
    t(c, 'Yahoo Finance  ·  Reuters  ·  Bloomberg', W - M, H - HDR_H + 9.5 * mm, 'Carlito', 5.5, SUBT, 'right')
    t(c, 'The Peninsula  ·  Qatar Tribune', W - M, H - HDR_H + 5.5 * mm, 'Carlito', 5.5, SUBT, 'right')

    if report_status != "ok":
        t(c, 'Validation status: NEEDS REVIEW', W - M, H - HDR_H + 2.2 * mm, 'Carlito-Bold', 5.5, WARN, 'right')

    fr(c, 0, H - HDR_H, W, 1.5 * mm, CYAN)
    return H - HDR_H


def draw_footer(c, report_date):
    fr(c, 0, 0, W, FTR_H, BLUE)
    fr(c, 0, FTR_H - 0.7 * mm, W, 0.7 * mm, CYAN)
    t(
        c,
        'Sources: Yahoo Finance  ·  Reuters  ·  Bloomberg  ·  The Peninsula  ·  Qatar Tribune    |    Strictly Confidential — Doha Bank HNWI Clients Only. Not for redistribution.',
        M, 2 * mm, 'Carlito-Italic', 5, SUBT
    )
    t(c, f'Doha Bank Market Intelligence  ·  {report_date}', W - M, 2 * mm, 'Carlito', 5.5, WHITE, 'right')


def draw_kpi(c, y, kpis):
    if not kpis:
        return y

    cw = UW / len(kpis)
    barcols = [MUTED, UP, GOLD, BLUE, DOWN, CYAN]
    for i, k in enumerate(kpis):
        val = str(k.get('value', '—'))
        lbl = k.get('label', '')
        sub = k.get('sublabel', '')
        cx = M + i * cw

        fr(c, cx, y - KPI_H, cw, KPI_H, WHITE)
        sr(c, cx, y - KPI_H, cw, KPI_H, RULE_DK, 0.5)
        fr(c, cx, y - 2 * mm, cw, 2 * mm, NAVY)
        fr(c, cx, y - KPI_H, cw, 1.2 * mm, barcols[i % len(barcols)])

        vcol = UP if val.startswith('+') else DOWN if val.startswith('-') else NAVY
        t(c, val, cx + 3 * mm, y - 6.5 * mm, 'Caladea-Bold', 11, vcol)
        t(c, lbl, cx + 3 * mm, y - 9.5 * mm, 'Carlito-Bold', 6.5, TEXT)
        t(c, sub, cx + 3 * mm, y - 13 * mm, 'Carlito', 5.5, MUTED)

    return y - KPI_H - 2 * mm


def sec_hdr(c, x, y, title, w):
    fr(c, x, y - SEC_H, w, SEC_H, BLUE)
    fr(c, x, y - SEC_H, 2 * mm, SEC_H, CYAN)
    t(c, f'| {title}', x + 3 * mm, y - 3.6 * mm, 'Caladea-Bold', 7, WHITE)
    return y - SEC_H


def draw_table(c, x, y, hdrs, rows, tw, cws):
    fr(c, x, y - ROW_H, tw, ROW_H, TBLHDR)
    hl(c, x, y - ROW_H, x + tw, CYAN, 0.5)
    cx = x
    for i, (h, cw) in enumerate(zip(hdrs, cws)):
        if i == 0:
            t(c, h, cx + 2 * mm, y - ROW_H + 1.5 * mm, 'Carlito-Bold', 6, WHITE)
        else:
            t(c, h, cx + cw / 2, y - ROW_H + 1.5 * mm, 'Carlito-Bold', 6, WHITE, 'center')
        cx += cw

    y -= ROW_H
    for ri, row in enumerate(rows):
        bg = OFFWHT if ri % 2 == 0 else WHITE
        fr(c, x, y - ROW_H, tw, ROW_H, bg)
        hl(c, x, y - ROW_H, x + tw, RULE, 0.2)
        cx = x
        for ci, (cell, cw) in enumerate(zip(row, cws)):
            cell = str(cell)
            if ci == 0:
                t(c, cell, cx + 2 * mm, y - ROW_H + 1.5 * mm, 'Carlito-Bold', 7, TEXT, 'left', cw - 3 * mm)
            else:
                col = pct_col(cell)
                fw = 'Carlito-Bold' if ('%' in cell or 'bps' in cell or cell in ('—', 'Pegged', 'N/A')) else 'Carlito'
                t(c, cell, cx + cw - 1.5 * mm, y - ROW_H + 1.5 * mm, fw, 7, col, 'right')
            cx += cw
        y -= ROW_H

    hl(c, x, y, x + tw, RULE_DK, 0.4)
    return y - 1.5 * mm


def cw5(w):
    return [w * 0.37, w * 0.18, w * 0.15, w * 0.15, w * 0.15]


def draw_news_card(c, x, y, w, h, item):
    metric = str(item.get('metric', '—'))
    mlbl = str(item.get('metric_label', ''))
    src = str(item.get('source', ''))
    hl_txt = str(item.get('headline', ''))
    summ = str(item.get('summary', ''))

    BW = 18 * mm

    fr(c, x, y - h, w, h, WHITE)
    sr(c, x, y - h, w, h, RULE, 0.5)

    bx = x + w - BW
    fr(c, bx, y - h, BW, h, NAVY)
    fr(c, bx, y - h, 0.8 * mm, h, CYAN)

    msz = 9 if len(metric) <= 7 else (7.5 if len(metric) <= 10 else 6.5)
    t(c, metric, bx + BW / 2 + 0.4 * mm, y - h / 2 + 1.5 * mm, 'Caladea-Bold', msz, CYAN, 'center')

    mw = BW - 3 * mm
    if c.stringWidth(mlbl, 'Carlito', 5) <= mw:
        t(c, mlbl, bx + BW / 2 + 0.4 * mm, y - h + 2.5 * mm, 'Carlito', 5, SUBT, 'center')
    else:
        words = mlbl.split()
        ln1 = ''
        ln2 = ''
        for w_ in words:
            test = (ln1 + ' ' + w_).strip()
            if c.stringWidth(test, 'Carlito', 5) <= mw:
                ln1 = test
            else:
                ln2 = (ln2 + ' ' + w_).strip()
        t(c, ln1, bx + BW / 2 + 0.4 * mm, y - h + 5 * mm, 'Carlito', 5, SUBT, 'center')
        t(c, ln2, bx + BW / 2 + 0.4 * mm, y - h + 2.5 * mm, 'Carlito', 5, SUBT, 'center')

    tx = x + 3 * mm
    tw2 = w - BW - 5 * mm

    hl_bot = ml(c, hl_txt, tx, y - 3 * mm, 'Caladea-Bold', 7.5, TEXT, tw2, 2.7 * mm, 2)
    t(c, src.upper(), tx, hl_bot - 2 * mm, 'Carlito-Bold', 5.5, CYAN)
    hl(c, tx, hl_bot - 3.2 * mm, tx + tw2, RULE, 0.25)
    ml(c, summ, tx, hl_bot - 5.5 * mm, 'Carlito-Italic', 6, MUTED, tw2, 2.3 * mm, 4)


def draw_news_grid(c, x, y, title, items, total_w, rows_count, card_h, card_gap=2 * mm):
    y = sec_hdr(c, x, y, title, total_w)
    y -= 1.5 * mm

    card_w = (total_w - card_gap) / 2
    max_cards = rows_count * 2

    for i, item in enumerate(items[:max_cards]):
        row = i // 2
        col = i % 2
        cx = x + col * (card_w + card_gap)
        cy = y - row * (card_h + card_gap)
        draw_news_card(c, cx, cy, card_w, card_h, item)

    return y - rows_count * (card_h + card_gap)


def section_rows(data, sec):
    out = []
    for r in data.get(sec, []):
        px = r.get('px_last', 'N/A')
        out.append([
            r.get('name', ''),
            clean_px(px),
            r.get('change_1d', 'N/A'),
            r.get('mtd', 'N/A'),
            r.get('ytd', 'N/A'),
        ])
    return out


def page1(c, report_date, generated_display_time, data):
    fr(c, 0, 0, W, H, WHITE)
    top = draw_header(
        c,
        report_date,
        generated_display_time,
        1,
        2,
        data.get("report_status", "ok")
    )
    y = top - 1.5 * mm

    y = draw_kpi(c, y, data.get('kpis', []))

    cw2 = (UW - 4 * mm) / 2
    xL = M
    xR = M + cw2 + 4 * mm

    yL = y
    yL = sec_hdr(c, xL, yL, 'GLOBAL INDICES', cw2)
    yL = draw_table(c, xL, yL, ['Market / Index', 'PX Last', '1D %', 'MTD %', 'YTD %'], section_rows(data, 'global_indices'), cw2, cw5(cw2))
    yL -= GAP
    yL = sec_hdr(c, xL, yL, 'SPOT CURRENCY', cw2)
    yL = draw_table(c, xL, yL, ['Pair', 'PX Last', '1D %', 'MTD %', 'YTD %'], section_rows(data, 'spot_currency'), cw2, cw5(cw2))
    yL -= GAP
    yL = sec_hdr(c, xL, yL, 'QAR CROSS RATES', cw2)
    yL = draw_table(c, xL, yL, ['Pair', 'PX Last', '1D %', 'MTD %', 'YTD %'], section_rows(data, 'qar_cross_rates'), cw2, cw5(cw2))
    yL -= GAP
    yL = sec_hdr(c, xL, yL, 'FIXED INCOME — UST YIELDS', cw2)
    yL = draw_table(c, xL, yL, ['Instrument', 'Yield', '1D Chg', 'MTD Chg', 'YTD Chg'], section_rows(data, 'fixed_income'), cw2, cw5(cw2))

    yR = y
    yR = sec_hdr(c, xR, yR, 'GCC & REGIONAL INDICES', cw2)
    yR = draw_table(c, xR, yR, ['Market / Index', 'PX Last', '1D %', 'MTD %', 'YTD %'], section_rows(data, 'gcc_indices'), cw2, cw5(cw2))
    yR -= GAP
    yR = sec_hdr(c, xR, yR, 'QATARI BANKS', cw2)
    yR = draw_table(c, xR, yR, ['Bank', 'PX Last', '1D %', 'MTD %', 'YTD %'], section_rows(data, 'qatari_banks'), cw2, cw5(cw2))
    yR -= GAP
    yR = sec_hdr(c, xR, yR, 'COMMODITIES & ENERGY', cw2)
    yR = draw_table(c, xR, yR, ['Asset', 'PX Last', '1D %', 'MTD %', 'YTD %'], section_rows(data, 'commodities'), cw2, cw5(cw2))

    draw_footer(c, report_date)


def page2(c, report_date, generated_display_time, global_news, qatar_news, report_status="ok"):
    fr(c, 0, 0, W, H, WHITE)
    top = draw_header(c, report_date, generated_display_time, 2, 2, report_status)
    y = top - 2 * mm

    avail = y - FTR_H - 3 * mm
    card_gap = 2 * mm
    between_sections = 3 * mm
    header_space = (2 * SEC_H) + 1.5 * mm + between_sections
    row_space = avail - header_space

    total_rows = 5
    total_internal_gaps = 4 * card_gap
    unit_h = (row_space - total_internal_gaps) / total_rows

    y = draw_news_grid(
        c, M, y,
        'REGIONAL & GLOBAL NEWS',
        global_news,
        UW,
        rows_count=3,
        card_h=unit_h,
        card_gap=card_gap
    )

    y -= between_sections

    y = draw_news_grid(
        c, M, y,
        'QATAR NEWS',
        qatar_news,
        UW,
        rows_count=2,
        card_h=unit_h,
        card_gap=card_gap
    )

    draw_footer(c, report_date)


def generate(data, output_path):
    report_date = data.get('config', {}).get('report_date', dt.today().strftime('%d %B %Y'))
    generated_display_time = data.get('generated_display_time', '07:00 AST')
    report_status = data.get('report_status', 'ok')

    c = pdfcanvas.Canvas(output_path, pagesize=landscape(A4))
    c.setTitle(f'Doha Bank Market Intelligence — {report_date}')
    c.setAuthor('Doha Bank — Automated Market Intelligence System')

    page1(c, report_date, generated_display_time, data)
    c.showPage()

    page2(
        c,
        report_date,
        generated_display_time,
        data.get('global_news', []),
        data.get('qatar_news', []),
        report_status
    )
    c.showPage()

    c.save()
    sz = os.path.getsize(output_path)
    print(f'PDF: {output_path}  |  {sz/1024:.0f} KB')
    return output_path


if __name__ == '__main__':
    if len(sys.argv) >= 3:
        data_path = sys.argv[1]
        out_path = sys.argv[2]
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        generate(data, out_path)
    else:
        print("Usage: python pdf_generator.py market_data.json report.pdf")
        raise SystemExit(1)
