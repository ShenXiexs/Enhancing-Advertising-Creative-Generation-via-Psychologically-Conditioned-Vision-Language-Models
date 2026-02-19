# Step1 背景 Prompt 生成逻辑（Big Five & Schwartz Value）

## 0.1 Latest behavior (2026-02 update)
- `concat` no longer appends persona text to the end.
- For Big Five / Schwartz with `target` wording, `concat` now builds:
  - `prompt: <Target audience sentence(s)> <scene description>`
- This front-loads audience semantics so the effective text sent to ComfyUI starts with target-audience cues.
- `inline` behavior stays the same: persona guidance is placed inside system prompt before generation.

## 0. 先澄清：Step1 vs Step2
- Step1（`create_promo_titles.py`）生成短标题，输出 `out_step1/step1_titles.xlsx`。
- 背景 prompt 实际在 `create_categorical_prompts.py` 生成，输出 `out_step1/step1_prompts_*.xlsx`（README 里叫 Step2 Prompt）。
- 本文聚焦：背景 prompt 的来源 + Big Five / Schwartz persona 的拼接方式。

## 1. 发送给 Ollama 的 system prompt 是怎么构成的
### 1.1 Base / Task / Tail（固定模板）
```text
You are an art director for product photography and image editing.

INPUTS: one product PHOTO.

TASK: Return EXACTLY FOUR English sentence that describes the environment/background AROUND the product, while keeping the product itself unchanged and fully visible. Avoid generic phrases like "on a clean white background".

Use cinematic lighting, depth, and realistic shadows. Include 3-8 tasteful props only when appropriate, and describe at least two concrete scene elements. No people, no on-image text, no logos, no clutter. English only, ending with "4k".
```
说明：Tail 由两部分组成：风格约束 + 4k 结尾。可用 `--style-constraints off` 关闭风格约束，用 `--end-with-4k off` 关闭 4k 结尾。

### 1.2 Triad 路由（按 super_category 选择风格）
- super_category 来自 `step_one_to_super_category_map.csv`。
- triad 路由表：`step_one_triad_prompts_22cats.csv`（每个类目 1-3 个风格）。
- 风格解释来自 `step_one_background_description.csv`。
- 如果该类目有 triad 记录，会插入以下片段：
```text
Choose ONE background style by product type:
- {Style A} — {Description A}
- {Style B} — {Description B}
- {Style C} — {Description C}
```

### 1.3 Persona 叠加方式（inline / concat）
- inline：persona 插入到 system prompt 中（位置在 TASK 之前）。
- concat：
  - Big Five / Schwartz (`target`): `prompt: <Target audience ...> <scene description>`
  - Other persona styles: keep compatibility path and still compose a final text prompt for rendering.

### 1.4 Ollama 请求形态（`create_categorical_prompts.py`）
```json
{
  "model": "qwen2.5vl:<7b|32b>",
  "messages": [
    {"role": "system", "content": "<SYSTEM_PROMPT>"},
    {"role": "user", "content": "", "images": ["<BASE64_IMAGE>"]}
  ],
  "stream": false,
  "options": {"num_predict": 160, "temperature": 0.5, "top_p": 0.9, "repeat_penalty": 1.1}
}
```
备注：如果 Ollama 不可用或图片失败，则回退到固定 prompt（`--end-with-4k off` 时不会带 "4k"）：
```text
A premium studio scene with textured materials and controlled highlights, realistic shadows, 4k
```

## 2. Big Five persona prompt（两种模式都保留）
- legacy 模式：沿用原始文本（默认）。参数：`--big5-persona-style legacy`
- target 模式：新增 Target Audience Persona Instruction 文本。参数：`--big5-persona-style target`

### 2.1 Legacy 模式模板（`--big5-persona-style legacy`）
Plan A: 
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- {Trait} ({Level}): the picture tends to be {big5_do}
  This picture does not tend to be: {big5_avoid}
```

(Important note - separate block)
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- {Trait} ({Level}): the picture tends to be {big5_do}
This picture does not tend to be:
- {big5_avoid}
```

### 2.2 Target 模式模板（`--big5-persona-style target`）
Plan A: 
```text
[Target Audience Persona Instruction]
Target audience Big Five: {Trait} ({Level}).
{big5_do_as_audience}
{big5_avoid_as_audience}
```

(Important note - separate block)
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: {Trait} ({Level}).
{big5_do_as_audience}
{big5_avoid_as_audience}
```

### 2.3 Big Five 各类型的实际文本（Legacy + Target）
#### Openness / High
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Openness (High): the picture tends to be vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
  This picture does not tend to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Openness (High): the picture tends to be vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
This picture does not tend to be:
- closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Openness (High).
This audience tends to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
This audience does not tend to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Openness (High).
This audience tends to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
This audience does not tend to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
```

#### Openness / Low
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Openness (Low): the picture tends to be closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
  This picture does not tend to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Openness (Low): the picture tends to be closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
This picture does not tend to be:
- vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Openness (Low).
This audience tends to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
This audience does not tend to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Openness (Low).
This audience tends to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention
This audience does not tend to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
```

#### Conscientiousness / High
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Conscientiousness (High): the picture tends to be self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
  This picture does not tend to be: self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Conscientiousness (High): the picture tends to be self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
This picture does not tend to be:
- self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Conscientiousness (High).
This audience tends to be: self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
This audience does not tend to be: self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Conscientiousness (High).
This audience tends to be: self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
This audience does not tend to be: self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
```

#### Conscientiousness / Low
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Conscientiousness (Low): the picture tends to be self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
  This picture does not tend to be: self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Conscientiousness (Low): the picture tends to be self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
This picture does not tend to be:
- self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Conscientiousness (Low).
This audience tends to be: self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
This audience does not tend to be: self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Conscientiousness (Low).
This audience tends to be: self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
This audience does not tend to be: self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
```

#### Extraversion / High
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Extraversion (High): the picture tends to be friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
  This picture does not tend to be: reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Extraversion (High): the picture tends to be friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
This picture does not tend to be:
- reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Extraversion (High).
This audience tends to be: friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
This audience does not tend to be: reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Extraversion (High).
This audience tends to be: friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
This audience does not tend to be: reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
```

#### Extraversion / Low
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Extraversion (Low): the picture tends to be reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
  This picture does not tend to be: friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Extraversion (Low): the picture tends to be reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
This picture does not tend to be:
- friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Extraversion (Low).
This audience tends to be: reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
This audience does not tend to be: friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Extraversion (Low).
This audience tends to be: reserved, solitary, submissive, passive, calm, serious-minded, low-activity, avoids attention, unhurried, content alone
This audience does not tend to be: friendly, gregarious, assertive, confident, high-energy, excitement-seeking, cheerful, optimistic, outgoing, socially engaged
```

#### Agreeableness / High
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Agreeableness (High): the picture tends to be trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
  This picture does not tend to be: distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Agreeableness (High): the picture tends to be trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
This picture does not tend to be:
- distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Agreeableness (High).
This audience tends to be: trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
This audience does not tend to be: distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Agreeableness (High).
This audience tends to be: trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
This audience does not tend to be: distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
```

#### Agreeableness / Low
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Agreeableness (Low): the picture tends to be distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
  This picture does not tend to be: trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Agreeableness (Low): the picture tends to be distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
This picture does not tend to be:
- trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Agreeableness (Low).
This audience tends to be: distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
This audience does not tend to be: trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Agreeableness (Low).
This audience tends to be: distrustful, immoral, selfish, competitive, arrogant, apathetic, self-serving, exploitative, insensitive, one-upmanship
This audience does not tend to be: trusting, moral, altruistic, cooperative, modest, sympathetic, generous, humble, good listener, team-oriented, compassionate
```

#### Neuroticism / High
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Neuroticism (High): the picture tends to be anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
  This picture does not tend to be: calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Neuroticism (High): the picture tends to be anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
This picture does not tend to be:
- calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Neuroticism (High).
This audience tends to be: anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
This audience does not tend to be: calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Neuroticism (High).
This audience tends to be: anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
This audience does not tend to be: calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable
```

#### Neuroticism / Low
Legacy / Plan A:
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Neuroticism (Low): the picture tends to be calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable, even-keeled
  This picture does not tend to be: anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Legacy / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
- Neuroticism (Low): the picture tends to be calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable, even-keeled
This picture does not tend to be:
- anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
```

Target / Plan A:
```text
[Target Audience Persona Instruction]
Target audience Big Five: Neuroticism (Low).
This audience tends to be: calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable, even-keeled
This audience does not tend to be: anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
```

Important (separate):
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

Target / Plan B:
```text
[Tone Hints]
Blend these Big Five cues into tone and word choice; keep product facts unchanged.
Focus on:
Target audience Big Five: Neuroticism (Low).
This audience tends to be: calm, content, self-assured, moderate, resilient, steady, emotionally stable, relaxed, even-keeled, unflappable, even-keeled
This audience does not tend to be: anxious, on-edge, worry-prone, irritable, easily angered, depressed, self-conscious, overwhelmed, vulnerable, emotionally volatile
```

## 3. Schwartz Value persona prompt（两种模式都保留）
- legacy 模式：原始 'You prioritize...' 文本（默认）。参数：`--schwartz-persona-style legacy`
- target 模式：新增 Target audience + This audience tends to be 文本。参数：`--schwartz-persona-style target`

### 3.1 固定参考行（每个 Schwartz 值都会包含）
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
```

(Important note - separate block)
```text
Important:
- This affects tone only; do not invent or alter product facts.
```

### 3.2 Legacy 模板（`--schwartz-persona-style legacy`）
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of {value_type} above all other values, which signifies {schwartz_value_do}
```

### 3.3 Target 模板（`--schwartz-persona-style target`）
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize {value_type} (Schwartz value).
This audience tends to be: {schwartz_value_do}
```

### 3.4 各 Schwartz 值的实际文本（Legacy + Target）
#### Universalism
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Universalism above all other values, which signifies Promoting justice, equality, and environmental protection. Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Universalism (Schwartz value).
This audience tends to be: Promoting justice, equality, and environmental protection. Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
```

#### Benevolence
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Benevolence above all other values, which signifies Prioritizing close social relationships and the welfare of others.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Benevolence (Schwartz value).
This audience tends to be: Prioritizing close social relationships and the welfare of others.
```

#### Power
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Power above all other values, which signifies Seeking dominance, control, and prestige.  Refers to social status and prestige, control or dominance over people and resources.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Power (Schwartz value).
This audience tends to be: Seeking dominance, control, and prestige.  Refers to social status and prestige, control or dominance over people and resources.
```

#### Achievement
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Achievement above all other values, which signifies Striving for personal success through competence.  Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Achievement (Schwartz value).
This audience tends to be: Striving for personal success through competence.  Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
```

#### Tradition
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Tradition above all other values, which signifies Respecting and preserving cultural and religious heritage.  Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Tradition (Schwartz value).
This audience tends to be: Respecting and preserving cultural and religious heritage.  Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
```

#### Conformity
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Conformity above all other values, which signifies Restricting actions that might disrupt social harmony.  Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Conformity (Schwartz value).
This audience tends to be: Restricting actions that might disrupt social harmony.  Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
```

#### Security
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Security above all other values, which signifies Seeking safety, stability, and social order.  Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Security (Schwartz value).
This audience tends to be: Seeking safety, stability, and social order.  Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
```

#### Self-Direction
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Self-Direction above all other values, which signifies Valuing autonomy in thought and decision-making.  Refers to independent thought and action-choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Self-Direction (Schwartz value).
This audience tends to be: Valuing autonomy in thought and decision-making.  Refers to independent thought and action-choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
```

#### Stimulation
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Stimulation above all other values, which signifies Seeking excitement, novelty, and challenges.  Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Stimulation (Schwartz value).
This audience tends to be: Seeking excitement, novelty, and challenges.  Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
```

#### Hedonism
Legacy:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
This picture tends to prioritize the value of Hedonism above all other values, which signifies Seeking pleasure, enjoyment, and the pursuit of personal.  Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
```

Target:
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: Refers to understanding, appreciating, tolerating, and protecting the welfare of all people and nature. For example: social justice, broad-mindedness, world peace, wisdom, a world of beauty, unity with nature, environmental protection, fairness.
2. Benevolence: Refers to preserving and enhancing the welfare of those with whom one is in frequent personal contact. For example: helpful, forgiving, loyal, honest, true friendship.
3. Power: Refers to social status and prestige, control or dominance over people and resources. For example: social power, wealth, authority.
4. Achievement: Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
5. Tradition: Refers to respect, commitment, and acceptance of the customs and ideas provided by one's culture or religion. For example: accepting my portion in life, devotion, respect for tradition, humbleness, moderation.
6. Conformity: Refers to the restraint of actions, inclinations, and impulses that may upset or harm others and violate social expectations or norms. For example: obedient, self-disciplined, polite, honoring parents and elders.
7. Security: Refers to the safety, harmony, and stability of society, relationships, and self. For example: family security, national security, social order, cleanliness, reciprocation of favors.
8. Self-Direction: Refers to independent thought and action - choosing, creating, exploring. For example: creativity, curiosity, freedom, independence, choosing own goals.
9. Stimulation: Refers to excitement, novelty, and challenge in life. For example: a varied life, an exciting life, daring.
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
For this ad picture:
Target audience: people who prioritize Hedonism (Schwartz value).
This audience tends to be: Seeking pleasure, enjoyment, and the pursuit of personal.  Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.
```

## 4. 可复现性 / 随机数种子检查
- `create_categorical_prompts.py` 有 `--seed`，会设置 Python/NumPy/Torch 的随机种子，但没有把 seed 传给 Ollama。
  - 当前 Ollama 调用使用 `temperature=0.5, top_p=0.9`，**不保证确定性**。
- ComfyUI 侧的随机性由 workflow 控制：`promo_banner_v3_api.json` 的 KSampler 节点种子固定为 `123`，脚本没有覆盖。
  - 在同一工作流/模型/版本下可复现；如果模型或节点版本变化，结果仍可能不同。

## 5. Prompt 方式与组数速查（Small Pipeline）

### 5.1 目前有哪些 Prompt 生成方式

#### A. 人格来源（`--persona-kind`，由 pipeline 内部控制）
- `none`：无人格基线
- `big5`：Big Five 人格
- `schwartz`：Schwartz 价值观

#### B. 人格注入方式（`--persona-mode`）
- `inline`：人格说明融合到 system prompt（生成前融合）
- `concat`：在最终 `prompt:` 文本前拼接人格信息
- `both`：`concat + inline` 都跑

#### C. Big Five 人格文案风格（`--big5-persona-style`）
- `legacy`：描述“这张图倾向于……”
- `target`：描述“目标人群倾向于……”

#### D. Schwartz 人格文案风格（`--schwartz-persona-style`）
- `legacy`：描述“这张图优先某价值观……”
- `target`：描述“目标人群优先某价值观……”

#### E. concat 人格拼接格式（`--concat-persona-format`）
- `lead`：前置简短目标人群引导语
- `full`：前置完整人格说明块

说明：
- `concat` 时人格特质已前置。
- `inline` 保持“融合到 system prompt”不变。

#### F. 场景风格变体（`--style-variants`）
- `style`：有风格约束（默认含 4k）
- `no-style`：无风格约束 + 4k（并禁用 triad）
- `both`：`style + no-style`
- `custom`：按 `--style-constraints` 和 `--end-with-4k` 自定义

### 5.2 `run_big5_schwartz_small_pipeline.py` 默认会生成多少组

默认参数下：
- `style-variants=both`（2 个视觉变体）
- `persona-mode=inline`（1 个人格注入方式）
- Big5 共 10 组（5 traits × High/Low）
- Schwartz 共 10 组
- `skip-nopersona=False`（含 1 组无人格）

所以：
- 每个视觉变体：`1 + 10 + 10 = 21` 组
- 总组数：`21 × 2 = 42` 组

如果用默认 `step1-sample-num=10`，总 prompt 条数约为：
- `42 × 10 = 420` 条（每组一个 xlsx，内含 10 条）

### 5.3 通用组数公式

记：
- `V` = 视觉变体数（`style=1`, `no-style=1`, `both=2`, `custom=1`）
- `M` = 人格注入方式数（`inline/concat=1`, `both=2`）
- `B` = Big5 组数（固定 10）
- `S` = Schwartz 组数（默认 10，可被 `--limit-schwartz` 限制）
- `N` = 是否包含 no-persona（包含=1，跳过=0）

总组数：
- `Total = V × (N + M × (B + S))`

默认即：
- `2 × (1 + 1 × (10 + 10)) = 42`

## 6. 各类 Prompt 示例（结构示例）

说明：
- 下面是结构示例，字段内容会随商品图、类目和模型输出变化。
- `inline` 示例看的是 system prompt（发给 VLM 前）。
- `concat` 示例看的是最终写入 Excel 的 `prompt` 文本（发给 ComfyUI）。

### 6.1 无人格（`persona-kind=none`）

system prompt（示例）：
```text
You are an art director for product photography and image editing.

INPUTS: one product PHOTO.

Choose ONE background style by product type:
- Clean Minimal — neutral matte surfaces with restrained props
- Natural Lifestyle — warm daylight, soft textures, home context

TASK: Return EXACTLY FOUR English sentence that describes the environment/background AROUND the product, while keeping the product itself unchanged and fully visible. Avoid generic phrases like "on a clean white background".

Use cinematic lighting, depth, and realistic shadows. Include 3-8 tasteful props only when appropriate, and describe at least two concrete scene elements. No people, no on-image text, no logos, no clutter. English only, ending with "4k".
```

最终 `prompt`（示例）：
```text
A brushed concrete tabletop anchors the product, with a warm side light carving realistic shadows across fine dust texture. A softly blurred linen backdrop and a muted ceramic tray add depth without competing for attention. Thin rim light defines product edges while keeping materials true to life. The composition stays uncluttered and premium, 4k.
```

### 6.2 Big Five `target` + `inline`

system prompt 中的人格块（示例，Openness High）：
```text
[Target Audience Persona Instruction]
Target audience Big Five: Openness (High).
This audience tends to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive
This audience does not tend to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention

Important:
- This affects tone only; do not invent or alter product facts.
```

最终 `prompt`（示例，场景句本身不直接显式带人格块）：
```text
The product is framed in an exploratory gallery-like setting with layered translucent panels and directional reflections. A bold color accent in the background introduces novelty while preserving product realism. Sculpted light gradients and controlled shadows create an expressive yet clean atmosphere. The object remains fully visible and physically accurate, 4k.
```

### 6.3 Big Five `target` + `concat` + `lead`

最终 `prompt`（示例）：
```text
prompt: The product is targeted to audience with Big Five: Openness (High). This audience tends to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive. This audience does not tend to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention. A brushed concrete tabletop anchors the product, with a warm side light carving realistic shadows across fine dust texture. A softly blurred linen backdrop and a muted ceramic tray add depth without competing for attention. Thin rim light defines product edges while keeping materials true to life. The composition stays uncluttered and premium, 4k.
```

### 6.4 Big Five `target` + `concat` + `full`

最终 `prompt`（示例）：
```text
prompt: [Target Audience Persona Instruction] Target audience Big Five: Openness (High). This audience tends to be: vivid imagination, artistic, emotionally expressive, adventurous, intellectually curious, liberal-minded, novelty-seeking, open to new experiences, self-expressive This audience does not tend to be: closed-off, low in imagination, little artistic interest, stoic, timid, less intellectually curious, conservative, risk-averse, comfort-zone oriented, avoids attention Important: - This affects tone only; do not invent or alter product facts. A brushed concrete tabletop anchors the product, with a warm side light carving realistic shadows across fine dust texture. A softly blurred linen backdrop and a muted ceramic tray add depth without competing for attention. Thin rim light defines product edges while keeping materials true to life. The composition stays uncluttered and premium, 4k.
```

备注：`concat` 在代码里会压成单行，因此 `full` 模式中原本多行的人格块会变成单行前缀。

### 6.5 Big Five `legacy` 示例

`inline` 人格块（示例，Conscientiousness High）：
```text
[Persona Instruction]
Use the communication style of this Big Five profile. Reflect it in tone and descriptive emphasis only; keep product facts intact.
Traits:
- Conscientiousness (High): the picture tends to be self-efficacious, orderly, dutiful, achievement-striving, self-disciplined, cautious, organized, methodical, responsible, future-conscious
  This picture does not tend to be: self-doubting, disorderly, careless, low ambition, low self-control, reckless, irresponsible, present-focused, disorganized, inattentive
```

### 6.6 Schwartz `target` + `inline`

system prompt 中的人格块（示例，Hedonism）：
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: ...
...
10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.

For this ad picture:
Target audience: people who prioritize Hedonism (Schwartz value).
This audience tends to be: Seeking pleasure, enjoyment, and the pursuit of personal. Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life.

Important:
- This affects tone only; do not invent or alter product facts.
```

### 6.7 Schwartz `target` + `concat`（`lead/full`）

`lead`（示例）：
```text
prompt: The product is targeted to audience who prioritize Hedonism (Schwartz value). This audience tends to be: Seeking pleasure, enjoyment, and the pursuit of personal. Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life. A low-gloss marble surface reflects subtle highlights under warm cinematic sidelighting. A crystal glass prop and a folded satin cloth add indulgent context without distracting from the product. Deep background falloff and realistic cast shadows preserve depth and material realism. The frame remains clean, premium, and product-centric, 4k.
```

`full`（示例）：
```text
prompt: Picture Value: Ten basic values of Schwartz's theory: 1. Universalism: ... 2. Benevolence: ... ... 10. Hedonism: Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life. For this ad picture: Target audience: people who prioritize Hedonism (Schwartz value). This audience tends to be: Seeking pleasure, enjoyment, and the pursuit of personal. Refers to pleasure or sensuous gratification for oneself. For example: pleasure, enjoying life. Important: - This affects tone only; do not invent or alter product facts. A low-gloss marble surface reflects subtle highlights under warm cinematic sidelighting. A crystal glass prop and a folded satin cloth add indulgent context without distracting from the product. Deep background falloff and realistic cast shadows preserve depth and material realism. The frame remains clean, premium, and product-centric, 4k.
```

### 6.8 Schwartz `legacy` 示例

`inline` 人格块（示例，Achievement）：
```text
Picture Value: Ten basic values of Schwartz's theory:
1. Universalism: ...
...
10. Hedonism: ...

For this ad picture:
This picture tends to prioritize the value of Achievement above all other values, which signifies Striving for personal success through competence. Refers to personal success achieved through demonstrating competence according to social standards. For example: successful, capable, ambitious, influential.
```
