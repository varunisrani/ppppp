from typing import Dict, Any, List
import streamlit as st
import os
from datetime import datetime, timedelta, date
import json
import asyncio
from src.script_ingestion.coordinator import ScriptIngestionCoordinator
from src.character_breakdown.coordinator import CharacterBreakdownCoordinator
from src.scheduling.coordinator import SchedulingCoordinator
from src.budgeting.coordinator import BudgetingCoordinator
from src.storyboard.coordinator import StoryboardCoordinator
from src.one_liner.agents.one_linear_agent import OneLinerAgent
import logging
import plotly.express as px
import plotly.graph_objects as go
import networkx as nx
import numpy as np
import pandas as pd
import qrcode
from io import BytesIO
import base64
import httpx
import replicate

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure Streamlit file watcher
try:
    # Reduce file watch frequency
    st.config.set_option('server.fileWatcherType', 'poll')
    st.config.set_option('server.maxUploadSize', 200)
    st.config.set_option('server.maxMessageSize', 200)
except Exception as e:
    logger.warning(f"Could not configure file watcher: {e}")

# Initialize coordinators
script_coordinator = ScriptIngestionCoordinator()
character_coordinator = CharacterBreakdownCoordinator()
scheduling_coordinator = SchedulingCoordinator()
budgeting_coordinator = BudgetingCoordinator()
storyboard_coordinator = StoryboardCoordinator()

# Initialize agents
one_liner_agent = OneLinerAgent()

# Knowledge base storage
STORAGE_DIR = "static/storage"
os.makedirs(STORAGE_DIR, exist_ok=True)

def save_to_storage(data: dict, filename: str):
    """Save data to storage with timestamp."""
    filepath = os.path.join(STORAGE_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    return filepath

def load_from_storage(filename: str) -> dict:
    """Load data from storage."""
    filepath = os.path.join(STORAGE_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return json.load(f)
    return {}

def reset_storage():
    """Reset all storage files and directories."""
    try:
        # Reset JSON result files
        result_files = [
            'script_ingestion_results.json',
            'one_liner_results.json',
            'character_breakdown_results.json',
            'schedule_results.json',
            'budget_results.json',
            'storyboard_results.json'
        ]
        for file in result_files:
            filepath = os.path.join(STORAGE_DIR, file)
            if os.path.exists(filepath):
                os.remove(filepath)
        
        # Reset uploaded script
        script_path = os.path.join(STORAGE_DIR, 'uploaded_script.txt')
        if os.path.exists(script_path):
            os.remove(script_path)
        
        # Reset storyboards directory
        storyboards_dir = os.path.join(STORAGE_DIR, 'storyboards')
        if os.path.exists(storyboards_dir):
            import shutil
            shutil.rmtree(storyboards_dir)
            os.makedirs(storyboards_dir)  # Recreate empty directory
            
        # Reset session state
        if 'current_step' in st.session_state:
            st.session_state.current_step = 'upload'
            
        return True, "Storage reset successfully"
    except Exception as e:
        logger.error(f"Error resetting storage: {str(e)}", exc_info=True)
        return False, f"Error resetting storage: {str(e)}"

def configure_watchdog():
    """Configure watchdog settings for file system monitoring."""
    try:
        import watchdog.observers
        watchdog.observers.Observer.DEFAULT_OBSERVER_TIMEOUT = 10  # Increase timeout
        return True
    except Exception as e:
        logger.error(f"Error configuring watchdog: {e}")
        return False

def main():
    # Configure watchdog before starting the app
    configure_watchdog()
    
    st.title("Film Production Assistant")
    
    # Initialize session state
    if 'current_step' not in st.session_state:
        st.session_state.current_step = 'upload'
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    steps = ['Upload Script', 'Script Analysis', 'One-Liner', 'Character Breakdown', 'Schedule', 'Budget', 'Storyboard', 'Overview']
    current_step = st.sidebar.radio("Go to", steps)
    
    # Add reset button to sidebar
    st.sidebar.markdown("---")  # Add separator
    if st.sidebar.button("Reset All Data", type="secondary"):
        success, message = reset_storage()
        if success:
            st.sidebar.success(message)
            st.rerun()  # Rerun the app to reflect changes
        else:
            st.sidebar.error(message)
    
    if current_step == 'Upload Script':
        show_upload_page()
    elif current_step == 'Script Analysis':
        show_script_analysis()
    elif current_step == 'One-Liner':
        show_one_liner()
    elif current_step == 'Character Breakdown':
        show_character_breakdown()
    elif current_step == 'Schedule':
        show_schedule()
    elif current_step == 'Budget':
        show_budget()
    elif current_step == 'Storyboard':
        show_storyboard()
    elif current_step == 'Overview':
        show_overview()

def show_upload_page():
    st.header("Upload Script")
    
    # Add tabs for different input methods
    tab1, tab2 = st.tabs(["Upload File", "Paste Text"])
    
    with tab1:
        uploaded_file = st.file_uploader("Choose a script file", type=['txt'])
        
        if uploaded_file is not None:
            st.success(f"File uploaded: {uploaded_file.name}")
            col1, col2 = st.columns([1, 2])
            with col1:
                submit_button = st.button("Submit and Process", key="submit_file", type="primary")
            
            if submit_button:
                script_path = os.path.join(STORAGE_DIR, 'uploaded_script.txt')
                with open(script_path, 'wb') as f:
                    f.write(uploaded_file.getvalue())
                
                with st.spinner("Processing script through ingestion pipeline..."):
                    try:
                        # Read the script content
                        with open(script_path, 'r') as f:
                            script_content = f.read()
                        
                        # Process through script ingestion pipeline
                        script_data = asyncio.run(script_coordinator.process_script(script_content, validation_level="lenient"))
                        
                        if "error" in script_data:
                            st.error(f"Script processing failed: {script_data['error']}")
                            # Show detailed error information if available
                            with st.expander("View Error Details"):
                                if "details" in script_data:
                                    st.text("Error Details:")
                                    st.code(script_data["details"])
                                if "raw_response" in script_data:
                                    st.text("Raw Response:")
                                    st.code(script_data["raw_response"])
                                if "processing_log" in script_data:
                                    st.text("Processing Log:")
                                    st.json(script_data["processing_log"])
                        else:
                            save_to_storage(script_data, 'script_ingestion_results.json')
                            st.session_state.current_step = 'Script Analysis'
                            st.success("Script processed successfully! Redirecting to analysis...")
                            st.rerun()
                            
                    except Exception as e:
                        logger.error(f"Error processing script: {str(e)}", exc_info=True)
                        st.error(f"An unexpected error occurred: {str(e)}")
    
    with tab2:
        pasted_text = st.text_area("Paste your script here", height=400, placeholder="Enter your script text here...")
        
        col1, col2 = st.columns([1, 2])
        with col1:
            submit_text_button = st.button("Submit and Process", key="submit_text", type="primary", disabled=not bool(pasted_text))
        
        if submit_text_button and pasted_text:
            with st.spinner("Processing script through ingestion pipeline..."):
                try:
                    # Process through script ingestion pipeline
                    script_data = asyncio.run(script_coordinator.process_script(pasted_text, validation_level="lenient"))
                    
                    if "error" in script_data:
                        st.error(f"Script processing failed: {script_data['error']}")
                        # Show detailed error information if available
                        with st.expander("View Error Details"):
                            if "details" in script_data:
                                st.text("Error Details:")
                                st.code(script_data["details"])
                            if "raw_response" in script_data:
                                st.text("Raw Response:")
                                st.code(script_data["raw_response"])
                            if "processing_log" in script_data:
                                st.text("Processing Log:")
                                st.json(script_data["processing_log"])
                    else:
                        save_to_storage(script_data, 'script_ingestion_results.json')
                        st.session_state.current_step = 'Script Analysis'
                        st.success("Script processed successfully! Redirecting to analysis...")
                        st.rerun()
                        
                except Exception as e:
                    logger.error(f"Error processing script: {str(e)}", exc_info=True)
                    st.error(f"An unexpected error occurred: {str(e)}")

def show_script_analysis():
    st.header("Script Analysis")
    results = load_from_storage('script_ingestion_results.json')
    
    if not results:
        st.warning("Please upload and process a script first.")
        if st.button("Go to Upload", type="primary"):
            st.session_state.current_step = 'Upload Script'
            st.rerun()
        return
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Timeline", "Scene Analysis", "Technical Requirements", 
        "Department Analysis", "Raw Data"
    ])
    
    with tab1:
        st.subheader("Script Timeline")
        if "parsed_data" in results and "timeline" in results["parsed_data"]:
            timeline = results["parsed_data"]["timeline"]
            
            # Display scene information in a table format
            st.write("### Scene Breakdown")
            scene_data = []
            for scene in timeline["scene_breakdown"]:
                scene_data.append({
                    "Scene": f"Scene {scene['scene_number']}",
                    "Start Time": scene['start_time'],
                    "End Time": scene['end_time'],
                    "Location": scene['location'],
                    "Characters": ", ".join(scene['characters']),
                    "Technical Complexity": scene['technical_complexity'],
                    "Setup Time": f"{scene['setup_time']} minutes"
                })
            
            st.table(scene_data)
            
            # Display scene duration statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Duration", timeline['total_duration'])
            with col2:
                st.metric("Average Scene Duration", f"{timeline['average_scene_duration']} min")
            with col3:
                st.metric("Total Scenes", len(timeline['scene_breakdown']))
    
    with tab2:
        st.subheader("Scene Analysis")
        if "parsed_data" in results and "scenes" in results["parsed_data"]:
            scenes = results["parsed_data"]["scenes"]
            
            # Display scene analysis in a structured format
            for scene in scenes:
                with st.expander(f"Scene {scene['scene_number']} - {scene['location']['place']} ({scene['time']})"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Description:**")
                        st.write(scene["description"])
                        st.write("**Main Characters:**")
                        st.write(", ".join(scene["main_characters"]))
                        
                        # Add complexity information
                        complexity_score = len(scene.get("technical_cues", [])) + \
                                        len(scene.get("main_characters", [])) + \
                                        sum(len(notes) for notes in scene.get("department_notes", {}).values())
                        st.write("**Complexity Score:**", complexity_score)
                        
                    with col2:
                        st.write("**Technical Cues:**")
                        for cue in scene.get("technical_cues", []):
                            st.write(f"- {cue}")
                        
                        st.write("**Department Notes:**")
                        for dept, notes in scene.get("department_notes", {}).items():
                            st.write(f"_{dept.title()}_:")
                            for note in notes:
                                st.write(f"  - {note}")
    
    with tab3:
        st.subheader("Technical Requirements")
        if "metadata" in results:
            metadata = results["metadata"]
            
            # Global requirements
            st.write("### Global Requirements")
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Equipment:**")
                for item in metadata.get("global_requirements", {}).get("equipment", []):
                    st.write(f"- {item}")
                
                st.write("**Props:**")
                for item in metadata.get("global_requirements", {}).get("props", []):
                    st.write(f"- {item}")
            
            with col2:
                st.write("**Special Effects:**")
                for item in metadata.get("global_requirements", {}).get("special_effects", []):
                    st.write(f"- {item}")
            
            # Technical requirements by scene
            st.write("### Technical Requirements by Scene")
            for scene_meta in metadata.get("scene_metadata", []):
                with st.expander(f"Scene {scene_meta['scene_number']} Technical Details"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write("**Lighting:**")
                        st.write(f"Type: {scene_meta['lighting']['type']}")
                        for req in scene_meta['lighting']['requirements']:
                            st.write(f"- {req}")
                        
                        st.write("**Props:**")
                        for category, items in scene_meta['props'].items():
                            if items:
                                st.write(f"_{category}_:")
                                for item in items:
                                    st.write(f"- {item}")
                    
                    with col2:
                        st.write("**Technical:**")
                        for category, items in scene_meta['technical'].items():
                            if items:
                                st.write(f"_{category}_:")
                                for item in items:
                                    st.write(f"- {item}")
    
    with tab4:
        st.subheader("Department Analysis")
        if "metadata" in results:
            metadata = results["metadata"]
            
            # Department workload analysis
            department_data = {}
            for scene_meta in metadata.get("scene_metadata", []):
                for dept, notes in scene_meta.get("department_notes", {}).items():
                    if dept not in department_data:
                        department_data[dept] = {"total_tasks": 0, "scenes": []}
                    department_data[dept]["total_tasks"] += len(notes)
                    department_data[dept]["scenes"].append(scene_meta["scene_number"])
            
            # Create department workload chart
            workload_data = {
                "Department": list(department_data.keys()),
                "Total Tasks": [data["total_tasks"] for data in department_data.values()],
                "Scene Coverage": [len(data["scenes"]) for data in department_data.values()]
            }
            
            fig = px.bar(
                workload_data,
                x="Department",
                y=["Total Tasks", "Scene Coverage"],
                title="Department Workload Analysis",
                barmode="group"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Department details
            for dept, data in department_data.items():
                with st.expander(f"{dept.title()} Department Details"):
                    st.write(f"**Total Tasks:** {data['total_tasks']}")
                    st.write(f"**Scenes Involved:** {', '.join(data['scenes'])}")
                    
                    # Show department notes by scene
                    st.write("**Notes by Scene:**")
                    for scene_meta in metadata.get("scene_metadata", []):
                        if dept in scene_meta.get("department_notes", {}):
                            st.write(f"_Scene {scene_meta['scene_number']}_:")
                            for note in scene_meta["department_notes"][dept]:
                                st.write(f"- {note}")
    
    with tab5:
        st.subheader("Raw Data")
        # Display parsed data
        if "parsed_data" in results:
            with st.expander("View Parsed Data", expanded=False):
                st.json(results["parsed_data"])
        
        # Display metadata
        if "metadata" in results:
            with st.expander("View Metadata", expanded=False):
                st.json(results["metadata"])
        
        # Navigation buttons
        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("Generate One-Liner", type="primary"):
                with st.spinner("Generating one-liner..."):
                    try:
                        one_liner_data = asyncio.run(one_liner_agent.process(
                            st.session_state.processed_data["script_analysis"]
                        ))
                        save_to_storage(one_liner_data, 'one_liner_results.json')
                        st.session_state.current_step = 'One-Liner'
                        st.success("One-liner generated successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error generating one-liner: {str(e)}")

def show_one_liner():
    st.markdown("## One-Liner Creation Module")
    
    # Check if script analysis is completed
    script_results = load_from_storage('script_ingestion_results.json')
    if not script_results:
        st.warning("Please complete script analysis first.")
        if st.button("Go to Script Analysis"):
            st.session_state.current_step = "Script Analysis"
            st.rerun()
        return
    
    # Load or generate one-liners
    one_liner_results = load_from_storage('one_liner_results.json')
    
    if not one_liner_results:
        if st.button("Generate One-Liners", type="primary"):
            with st.spinner("Generating one-liner summaries..."):
                try:
                    one_liner_results = one_liner_agent.process(script_results)
                    save_to_storage(one_liner_results, 'one_liner_results.json')
                    st.success("One-liners generated successfully!")
                    st.rerun()
                except Exception as e:
                    logger.error(f"Error generating one-liners: {str(e)}", exc_info=True)
                    st.error(f"Failed to generate one-liners: {str(e)}")
                    if hasattr(e, 'details'):
                        with st.expander("View Error Details"):
                            st.code(e.details)
                    return
        return
    
    # Display one-liners in a more organized way
    st.markdown("### Scene Summaries")
    
    # Extract scenes from the results
    scenes = one_liner_results.get("scenes", [])
    
    # Create columns for better organization
    col1, col2 = st.columns([3, 1])
    
    with col1:
        for scene in sorted(scenes, key=lambda x: x.get("scene_number", 0)):
            with st.container(border=True):
                st.markdown(f"**Scene {scene.get('scene_number', '?')}**")
                st.write(scene.get("one_liner", "No summary available"))
    
    with col2:
        st.markdown("### Export Options")
        
        # Add regenerate button at the top
        if st.button("🔄 Regenerate One-Liners", type="primary"):
            with st.spinner("Regenerating one-liner summaries..."):
                try:
                    one_liner_results = one_liner_agent.process(script_results)
                    save_to_storage(one_liner_results, 'one_liner_results.json')
                    st.success("One-liners regenerated successfully!")
                    st.rerun()
                except Exception as e:
                    logger.error(f"Error regenerating one-liners: {str(e)}", exc_info=True)
                    st.error(f"Failed to regenerate one-liners: {str(e)}")
                    if hasattr(e, 'details'):
                        with st.expander("View Error Details"):
                            st.code(e.details)
        
        st.markdown("---")  # Add a separator
        
        # Add download button for JSON
        json_str = json.dumps(one_liner_results, indent=2)
        st.download_button(
            label="Download One-Liners (JSON)",
            data=json_str,
            file_name="one_liners.json",
            mime="application/json",
            help="Download the one-liner summaries in JSON format"
        )
        
        # Add copy to clipboard option
        if st.button("Copy All to Clipboard"):
            # Format the text for clipboard
            clipboard_text = "\n\n".join([
                f"Scene {scene.get('scene_number', '?')}:\n{scene.get('one_liner', 'No summary available')}"
                for scene in sorted(scenes, key=lambda x: x.get("scene_number", 0))
            ])
            st.code(clipboard_text)
            st.success("Text copied to clipboard! Use the code block above to copy.")

def show_character_breakdown():
    st.header("Character Breakdown")
    
    # Initialize breakdown_results first
    breakdown_results = load_from_storage('character_breakdown_results.json')
    script_results = load_from_storage('script_ingestion_results.json')
    
    if not script_results:
        st.warning("Please upload and process a script first.")
        return
    
    # Check if we need to generate character breakdown
    if not breakdown_results:
        if st.button("Generate Character Breakdown", type="primary"):
            with st.spinner("Analyzing characters..."):
                try:
                    breakdown_results = asyncio.run(character_coordinator.process_script(script_results))
                    save_to_storage(breakdown_results, 'character_breakdown_results.json')
                    st.success("Character breakdown generated!")
                    st.rerun()
                except Exception as e:
                    logger.error(f"Error generating character breakdown: {str(e)}", exc_info=True)
                    st.error(f"An error occurred: {str(e)}")
        return
    
    # Add tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Character Profiles",
        "Arc & Relationships",
        "Scene Matrix",
        "Statistics",
        "Raw Data"
    ])
    
    with tab1:
        # Character Profiles View
        if breakdown_results and "characters" in breakdown_results:
            # Character selector
            selected_char = st.selectbox(
                "Select Character",
                list(breakdown_results["characters"].keys()),
                key="profile_char_select"
            )
            
            if selected_char:
                char_data = breakdown_results["characters"][selected_char]
                
                # Profile header
                st.title(selected_char)
                
                # Create subtabs for detailed information
                prof_tab1, prof_tab2, prof_tab3, prof_tab4 = st.tabs([
                    "Overview", "Dialogue & Actions", "Emotional Journey", "Technical Details"
                ])
                
                with prof_tab1:
                    # Basic information and objectives
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("### Character Overview")
                        if "objectives" in char_data:
                            objectives = char_data["objectives"]
                            st.write(f"**Main Objective:** {objectives.get('main_objective', 'N/A')}")
                    
                    with col2:
                        if "dialogue_analysis" in char_data:
                            dialogue = char_data["dialogue_analysis"]
                            st.write("### Dialogue Stats")
                            st.metric("Total Lines", dialogue.get("total_lines", 0))
                            st.metric("Total Words", dialogue.get("total_words", 0))
                            st.metric("Avg. Line Length", f"{dialogue.get('average_line_length', 0.0):.1f}")
                            st.metric("Vocabulary Complexity", f"{dialogue.get('vocabulary_complexity', 0.0):.2f}")
                    
                    # Scene objectives
                    if "objectives" in char_data and "scene_objectives" in char_data["objectives"]:
                        st.write("### Scene Objectives")
                        for obj in char_data["objectives"]["scene_objectives"]:
                            with st.container(border=True):
                                st.write(f"**Scene {obj.get('scene', 'N/A')}**")
                                st.write(f"Objective: {obj.get('objective', 'N/A')}")
                                st.write("Obstacles:")
                                for obstacle in obj.get("obstacles", []):
                                    st.write(f"- {obstacle}")
                                st.write(f"Outcome: {obj.get('outcome', 'N/A')}")
                
                with prof_tab2:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        # Dialogue patterns
                        if "dialogue_analysis" in char_data and "patterns" in char_data["dialogue_analysis"]:
                            patterns = char_data["dialogue_analysis"]["patterns"]
                            st.write("### Speech Patterns")
                            st.write(f"**Style:** {patterns.get('speech_style', 'N/A')}")
                            
                            st.write("**Common Phrases:**")
                            for phrase in patterns.get("common_phrases", []):
                                st.write(f"- {phrase}")
                            
                            st.write("**Emotional Markers:**")
                            for marker in patterns.get("emotional_markers", []):
                                st.write(f"- {marker}")
                    
                    with col2:
                        # Action sequences
                        if "action_sequences" in char_data:
                            st.write("### Action Sequences")
                            for action in char_data["action_sequences"]:
                                with st.container(border=True):
                                    st.write(f"**Scene {action.get('scene', 'N/A')}**")
                                    st.write(f"Sequence: {action.get('sequence', 'N/A')}")
                                    st.write(f"Type: {action.get('interaction_type', 'N/A')}")
                                    st.write(f"Emotional Context: {action.get('emotional_context', 'N/A')}")
                
                with prof_tab3:
                    # Emotional range and journey
                    if "emotional_range" in char_data:
                        emotional = char_data["emotional_range"]
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("### Emotional Profile")
                            st.write(f"**Primary Emotion:** {emotional.get('primary_emotion', 'N/A')}")
                            st.write("**Emotional Spectrum:**")
                            for emotion in emotional.get('emotional_spectrum', []):
                                st.write(f"- {emotion}")
                        
                        with col2:
                            # Create emotional journey visualization
                            if "emotional_journey" in emotional:
                                journey_data = {
                                    "Scene": [],
                                    "Emotion": [],
                                    "Intensity": [],
                                    "Trigger": []
                                }
                                
                                for point in emotional["emotional_journey"]:
                                    journey_data["Scene"].append(f"Scene {point.get('scene', 'N/A')}")
                                    journey_data["Emotion"].append(point.get('emotion', 'N/A'))
                                    journey_data["Intensity"].append(point.get('intensity', 0))
                                    journey_data["Trigger"].append(point.get('trigger', 'N/A'))
                                
                                fig = px.line(
                                    journey_data,
                                    x="Scene",
                                    y="Intensity",
                                    color="Emotion",
                                    title="Emotional Journey",
                                    hover_data=["Trigger"]
                                )
                                st.plotly_chart(fig, use_container_width=True)
                
                with prof_tab4:
                    # Technical details
                    tech_tab1, tech_tab2, tech_tab3 = st.tabs(["Props", "Costumes", "Makeup"])
                    
                    with tech_tab1:
                        if "props" in char_data:
                            st.write("### Props")
                            props = char_data["props"]
                            
                            st.write("**Base Props:**")
                            for prop in props.get("base", []):
                                st.write(f"- {prop}")
                            
                            st.write("### Props Timeline")
                            for entry in props.get("timeline", []):
                                with st.container(border=True):
                                    st.write(f"**Scene {entry.get('scene', 'N/A')}**")
                                    if entry.get("additions"):
                                        st.write("Added:")
                                        for prop in entry["additions"]:
                                            st.write(f"- {prop}")
                                    if entry.get("removals"):
                                        st.write("Removed:")
                                        for prop in entry["removals"]:
                                            st.write(f"- {prop}")
                    
                    with tech_tab2:
                        if "costumes" in char_data:
                            st.write("### Costumes")
                            for costume in char_data["costumes"]:
                                with st.container(border=True):
                                    if isinstance(costume, dict):
                                        st.write(f"**Scene:** {costume.get('scene', 'N/A')}")
                                        st.write(f"**Description:** {costume.get('description', 'N/A')}")
                                    else:
                                        st.write(costume)
                    
                    with tech_tab3:
                        if "makeup" in char_data:
                            st.write("### Makeup")
                            makeup = char_data["makeup"]
                            
                            st.write("**Base Makeup:**")
                            if isinstance(makeup.get("base"), dict):
                                st.write(makeup["base"].get("item", "None"))
                            
                            st.write("### Makeup Timeline")
                            for entry in makeup.get("timeline", []):
                                with st.container(border=True):
                                    st.write(f"**Scene {entry.get('scene', 'N/A')}**")
                                    if "changes" in entry:
                                        st.write(f"Changes: {entry['changes'].get('item', 'None')}")
                                    if "special_effects" in entry:
                                        st.write("Special Effects:")
                                        for effect in entry["special_effects"]:
                                            st.write(f"- {effect}")
    
    with tab2:
        if breakdown_results:
            # Relationship Network
            st.write("### Character Relationships")
            if "relationships" in breakdown_results:
                for rel_key, rel_data in breakdown_results["relationships"].items():
                    with st.expander(f"Relationship: {rel_key}"):
                        st.write(f"**Type:** {rel_data.get('type', 'N/A')}")
                        
                        st.write("**Dynamics:**")
                        for dynamic in rel_data.get("dynamics", []):
                            st.write(f"- {dynamic}")
                        
                        st.write("### Evolution")
                        for evolution in rel_data.get("evolution", []):
                            with st.container(border=True):
                                st.write(f"**Scene {evolution.get('scene', 'N/A')}**")
                                st.write(f"Change: {evolution.get('dynamic_change', 'N/A')}")
                                st.write(f"Trigger: {evolution.get('trigger', 'N/A')}")
                        
                        st.write("### Interactions")
                        for interaction in rel_data.get("interactions", []):
                            with st.container(border=True):
                                st.write(f"**Scene {interaction.get('scene', 'N/A')}**")
                                st.write(f"Type: {interaction.get('type', 'N/A')}")
                                st.write(f"Description: {interaction.get('description', 'N/A')}")
                                st.write(f"Emotional Impact: {interaction.get('emotional_impact', 'N/A')}")
                        
                        if "conflicts" in rel_data:
                            st.write("### Conflicts")
                            for conflict in rel_data["conflicts"]:
                                with st.container(border=True):
                                    st.write(f"**Scene {conflict.get('scene', 'N/A')}**")
                                    st.write(f"Conflict: {conflict.get('conflict', 'N/A')}")
                                    st.write(f"Resolution: {conflict.get('resolution', 'N/A')}")
    
    with tab3:
        if breakdown_results and "scene_matrix" in breakdown_results:
            st.write("### Scene Matrix")
            
            # Scene selector
            scenes = sorted(breakdown_results["scene_matrix"].keys(), key=lambda x: int(x))
            selected_scene = st.selectbox("Select Scene", [f"Scene {s}" for s in scenes])
            
            if selected_scene:
                scene_num = selected_scene.split()[1]
                scene_data = breakdown_results["scene_matrix"][scene_num]
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write("**Present Characters:**")
                    for char in scene_data.get("present_characters", []):
                        st.write(f"- {char}")
                    
                    st.write(f"**Emotional Atmosphere:** {scene_data.get('emotional_atmosphere', 'N/A')}")
                
                with col2:
                    st.write("**Key Developments:**")
                    for dev in scene_data.get("key_developments", []):
                        st.write(f"- {dev}")
                
                st.write("### Interactions")
                for interaction in scene_data.get("interactions", []):
                    with st.container(border=True):
                        st.write(f"**Characters:** {', '.join(interaction.get('characters', []))}")
                        st.write(f"**Type:** {interaction.get('type', 'N/A')}")
                        st.write(f"**Significance:** {interaction.get('significance', 0.0):.2f}")
    
    with tab4:
        if breakdown_results and "statistics" in breakdown_results:
            stats = breakdown_results["statistics"]
            
            st.write("### Overall Statistics")
            
            # Scene Statistics
            if "scene_stats" in stats:
                scene_stats = stats["scene_stats"]
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Scenes", scene_stats.get("total_scenes", 0))
                with col2:
                    st.metric("Avg Characters/Scene", f"{scene_stats.get('average_characters_per_scene', 0):.1f}")
                with col3:
                    st.metric("Total Interactions", scene_stats.get("total_interactions", 0))
            
            # Dialogue Statistics
            if "dialogue_stats" in stats:
                st.write("### Dialogue Statistics")
                dialogue_data = []
                for char, char_stats in stats["dialogue_stats"].items():
                    dialogue_data.append({
                        "Character": char,
                        "Total Lines": char_stats.get("total_lines", 0),
                        "Total Words": char_stats.get("total_words", 0),
                        "Avg Line Length": char_stats.get("average_line_length", 0),
                        "Vocabulary Complexity": char_stats.get("vocabulary_complexity", 0)
                    })
                
                if dialogue_data:
                    df = pd.DataFrame(dialogue_data)
                    st.dataframe(df, use_container_width=True)
            
            # Emotional Statistics
            if "emotional_stats" in stats:
                st.write("### Emotional Statistics")
                emotion_data = []
                for char, char_stats in stats["emotional_stats"].items():
                    emotion_data.append({
                        "Character": char,
                        "Primary Emotion": char_stats.get("primary_emotion", "N/A"),
                        "Emotional Variety": char_stats.get("emotional_variety", 0),
                        "Average Intensity": f"{char_stats.get('average_intensity', 0):.2f}"
                    })
                
                if emotion_data:
                    df = pd.DataFrame(emotion_data)
                    st.dataframe(df, use_container_width=True)
            
            # Technical Statistics
            if "technical_stats" in stats:
                st.write("### Technical Statistics")
                tech_stats = stats["technical_stats"]
                
                # Costume Changes
                st.write("**Costume Changes**")
                costume_data = []
                for char, char_stats in tech_stats["costume_changes"].items():
                    costume_data.append({
                        "Character": char,
                        "Total Changes": char_stats.get("total_changes", 0),
                        "Unique Costumes": char_stats.get("unique_costumes", 0)
                    })
                
                if costume_data:
                    df = pd.DataFrame(costume_data)
                    st.dataframe(df, use_container_width=True)
    
    with tab5:
        if breakdown_results:
            st.json(breakdown_results)
            
            st.download_button(
                "Download Full Analysis",
                data=json.dumps(breakdown_results, indent=2),
                file_name="character_breakdown.json",
                mime="application/json"
            )
    
    # Navigation buttons
    st.divider()
    col1, col2 = st.columns([1, 2])
    with col1:
        if st.button("Generate Production Schedule", type="primary"):
            with st.spinner("Analyzing characters and generating schedule..."):
                try:
                    # Load required data
                    script_results = load_from_storage('script_ingestion_results.json')
                    character_breakdown = load_from_storage('character_breakdown_results.json')
                    
                    # First ensure we have valid data
                    if not script_results or not character_breakdown:
                        st.error("Please complete script analysis and character breakdown first")
                        return
                        
                    # Get default constraints
                    location_constraints = {
                        "preferred_locations": [],
                        "avoid_weather": ["Rain", "Snow", "High Winds"]
                    }
                    
                    schedule_constraints = {
                        "max_hours_per_day": 12,
                        "meal_break_duration": 60,
                        "company_moves_per_day": 2
                    }
                    
                    # Generate schedule
                    schedule_data = asyncio.run(run_scheduling_pipeline(
                        script_results=script_results,
                        character_results=character_breakdown,
                        start_date=datetime.now().strftime("%Y-%m-%d"),
                        location_constraints=location_constraints,
                        schedule_constraints=schedule_constraints
                    ))
                    
                    if schedule_data:
                        # Save the schedule data
                        save_to_storage(schedule_data, 'schedule_results.json')
                        st.session_state.current_step = 'Schedule'
                        st.success("Schedule generated successfully!")
                        st.rerun()
                    else:
                        st.error("Failed to generate schedule - no data returned")
                        
                except Exception as e:
                    logger.error(f"Error generating schedule: {str(e)}", exc_info=True)
                    st.error(f"An error occurred while generating the schedule: {str(e)}")
                    if "is_generating" in st.session_state:
                        st.session_state.is_generating = False

async def run_scheduling_pipeline(script_results, character_results, start_date, location_constraints, schedule_constraints):
    """Run the complete scheduling pipeline asynchronously."""
    try:
        coordinator = SchedulingCoordinator()
        
        # Status placeholder for progress updates
        status = st.empty()
        progress = st.progress(0)
        
        # Step 1: Location Optimization
        status.text("Step 1/3: Optimizing shooting locations...")
        progress.progress(20)
        
        # Log input data for debugging
        logger.info(f"Starting scheduling pipeline with start_date: {start_date}")
        logger.info(f"Location constraints: {location_constraints}")
        logger.info(f"Schedule constraints: {schedule_constraints}")
        
        # Ensure script_results has the correct structure
        if isinstance(script_results, dict) and "parsed_data" in script_results:
            scene_data = script_results
        else:
            scene_data = {"parsed_data": {"scenes": script_results}} if isinstance(script_results, list) else {"parsed_data": {"scenes": []}}
        
        # Log the processed scene data
        logger.info(f"Number of scenes to process: {len(scene_data.get('parsed_data', {}).get('scenes', []))}")
        
        # Step 2: Generate Schedule
        status.text("Step 2/3: Generating initial schedule...")
        progress.progress(60)
        
        schedule_results = await coordinator.generate_schedule(
            scene_data=scene_data,
            crew_data=character_results,
            start_date=start_date,
            location_constraints=location_constraints,
            schedule_constraints=schedule_constraints
        )
        
        # Step 3: Finalize
        status.text("Step 3/3: Finalizing schedule...")
        progress.progress(90)
        
        if not schedule_results:
            raise ValueError("No schedule results generated")
            
        # Validate the schedule results
        if not isinstance(schedule_results, dict):
            raise ValueError(f"Invalid schedule results type: {type(schedule_results)}")
            
        required_keys = ["schedule", "summary"]
        missing_keys = [key for key in required_keys if key not in schedule_results]
        if missing_keys:
            raise ValueError(f"Missing required keys in schedule results: {missing_keys}")
        
        progress.progress(100)
        status.empty()
        
        logger.info("Schedule generation completed successfully")
        return schedule_results
        
    except Exception as e:
        logger.error(f"Error in scheduling pipeline: {str(e)}", exc_info=True)
        status.error(f"Error generating schedule: {str(e)}")
        progress.empty()
        raise

def show_schedule():
    st.title("Schedule View")
    
    # Initialize session state
    if "schedule_modified" not in st.session_state:
        st.session_state.schedule_modified = False
    if "dragged_scene" not in st.session_state:
        st.session_state.dragged_scene = None
    if "is_generating" not in st.session_state:
        st.session_state.is_generating = False
    
    # Create tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "Calendar View", "Schedule List", "Location Plan", "Crew Allocation", 
        "Equipment", "Department Schedules", "Call Sheets", "Raw Data"
    ])
    
    # Load schedule data
    schedule_data = load_from_storage('schedule_results.json')
    
    with tab1:
        st.write("## Calendar View")
        if schedule_data and "calendar_data" in schedule_data:
            calendar_data = schedule_data["calendar_data"]
            
            # Display calendar events
            st.write("### Shooting Schedule")
            for event in calendar_data.get("events", []):
                with st.container(border=True):
                    col1, col2 = st.columns([2,1])
                    with col1:
                        st.write(f"**{event['title']}**")
                        st.write(f"Time: {event['start'].split('T')[1]} - {event['end'].split('T')[1]}")
                        st.write(f"Location: {event['location']}")
                    with col2:
                        st.write("**Crew:**")
                        for crew_member in event.get("crew", []):
                            st.write(f"- {crew_member}")
                        st.write("**Equipment:**")
                        for equipment in event.get("equipment", []):
                            st.write(f"- {equipment}")
    
    with tab2:
        st.write("## Schedule List")
        if schedule_data and "schedule" in schedule_data:
            # Add Generate Budget button at the top
            col1, col2 = st.columns([1, 3])
            with col1:
                if st.button("💰 Generate Budget", type="primary"):
                    with st.spinner("Generating production budget..."):
                        try:
                            # Load required data
                            script_results = load_from_storage('script_ingestion_results.json')
                            character_results = load_from_storage('character_breakdown_results.json')
                            
                            if not script_results or not character_results:
                                st.error("Please complete script analysis and character breakdown first.")
                                return
                            
                            # Prepare production data
                            production_data = {
                                "script_metadata": script_results.get("metadata", {}),
                                "scene_count": len(script_results.get("parsed_data", {}).get("scenes", [])),
                                "character_count": len(character_results.get("characters", [])),
                                "schedule_days": len(schedule_data.get("schedule", [])),
                                "quality_level": "Medium"  # Default value
                            }
                            
                            # Prepare location data from schedule
                            location_data = {
                                "locations": [
                                    scene.get("location_id", "Unknown")
                                    for day in schedule_data.get("schedule", [])
                                    for scene in day.get("scenes", [])
                                ]
                            }
                            
                            # Prepare crew data
                            crew_data = {
                                "size": "Medium",  # Default value
                                "equipment_level": "Standard",  # Default value
                                "departments": ["Production", "Camera", "Lighting", "Sound", "Art", "Makeup", "Wardrobe"]
                            }
                            
                            # Default constraints
                            constraints = {
                                "quality_level": "Medium",
                                "equipment_preference": "Standard",
                                "crew_size": "Medium",
                                "schedule_days": len(schedule_data.get("schedule", [])),
                                "total_scenes": len(script_results.get("parsed_data", {}).get("scenes", [])),
                                "total_characters": len(character_results.get("characters", []))
                            }
                            
                            # Generate budget using coordinator
                            budget_results = asyncio.run(budgeting_coordinator.initialize_budget(
                                production_data=production_data,
                                location_data=location_data,
                                crew_data=crew_data,
                                constraints=constraints
                            ))
                            
                            save_to_storage(budget_results, 'budget_results.json')
                            st.success("Budget generated! Redirecting to Budget view...")
                            st.session_state.current_step = 'Budget'
                            st.rerun()
                        except Exception as e:
                            logger.error(f"Error generating budget: {str(e)}", exc_info=True)
                            st.error(f"An error occurred: {str(e)}")
            
            # Display schedule list
            for day in schedule_data["schedule"]:
                st.write(f"### Day {day['day']} - {day['date']}")
                for scene in day["scenes"]:
                    with st.container(border=True):
                        col1, col2, col3 = st.columns([2,1,1])
                        with col1:
                            st.write(f"**Scene {scene['scene_id']}**")
                            st.write(f"Location: {scene['location_id']}")
                            st.write(f"Time: {scene['start_time']} - {scene['end_time']}")
                        with col2:
                            st.write("**Setup:** " + scene['setup_time'])
                            st.write("**Wrap:** " + scene['wrap_time'])
                        with col3:
                            if scene.get("breaks"):
                                st.write("**Breaks:**")
                                for break_info in scene["breaks"]:
                                    st.write(f"{break_info['type']}: {break_info['start_time']} - {break_info['end_time']}")
    
    with tab3:
        st.write("## Location Plan")
        if schedule_data and "location_plan" in schedule_data:
            location_plan = schedule_data["location_plan"]
            
            # Display locations
            st.write("### Locations")
            for location in location_plan.get("locations", []):
                with st.expander(f"{location['name']} ({location['id']})"):
                    st.write(f"**Address:** {location['address']}")
                    st.write(f"**Scenes:** {', '.join(location['scenes'])}")
                    st.write("**Requirements:**")
                    for req in location["requirements"]:
                        st.write(f"- {req}")
                    st.write(f"**Setup Time:** {location['setup_time_minutes']} minutes")
                    st.write(f"**Wrap Time:** {location['wrap_time_minutes']} minutes")
                    
                    # Weather dependencies
                    if location["id"] in location_plan.get("weather_dependencies", {}):
                        weather = location_plan["weather_dependencies"][location["id"]]
                        st.write("### Weather Requirements")
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write("**Preferred Conditions:**")
                            for cond in weather["preferred_conditions"]:
                                st.write(f"- {cond}")
                        with col2:
                            st.write("**Avoid Conditions:**")
                            for cond in weather["avoid_conditions"]:
                                st.write(f"- {cond}")
                        st.write("**Seasonal Notes:**")
                        for note in weather["seasonal_notes"]:
                            st.write(f"- {note}")
            
            # Display optimization notes
            st.write("### Optimization Notes")
            for note in location_plan.get("optimization_notes", []):
                st.write(f"- {note}")
    
    with tab4:
        st.write("## Crew Allocation")
        if schedule_data and "crew_allocation" in schedule_data:
            crew_data = schedule_data["crew_allocation"]
            
            # Display crew assignments
            st.write("### Crew Assignments")
            for crew in crew_data.get("crew_assignments", []):
                with st.expander(f"{crew['crew_member']} - {crew['role']}"):
                    st.write(f"**Assigned Scenes:** {', '.join(crew['assigned_scenes'])}")
                    st.write(f"**Work Hours:** {crew['work_hours']}")
                    st.write(f"**Turnaround Hours:** {crew['turnaround_hours']}")
                    st.write(f"**Meal Break Interval:** {crew['meal_break_interval']} hours")
                    st.write("**Equipment Assigned:**")
                    for equipment in crew["equipment_assigned"]:
                        st.write(f"- {equipment}")
    
    with tab5:
        st.write("## Equipment")
        if schedule_data and "crew_allocation" in schedule_data:
            equipment_data = schedule_data["crew_allocation"].get("equipment_assignments", [])
            
            # Display equipment assignments
            for equipment in equipment_data:
                with st.container(border=True):
                    st.write(f"**{equipment['equipment_id']}** ({equipment['type']})")
                    st.write(f"Setup Time: {equipment['setup_time_minutes']} minutes")
                    st.write(f"Assigned Scenes: {', '.join(equipment['assigned_scenes'])}")
                    st.write(f"Assigned Crew: {', '.join(equipment['assigned_crew'])}")
    
    with tab6:
        st.write("## Department Schedules")
        if schedule_data and "crew_allocation" in schedule_data:
            dept_data = schedule_data["crew_allocation"].get("department_schedules", {})
            
            for dept_name, dept_info in dept_data.items():
                with st.expander(dept_name.title()):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Crew:**")
                        for crew in dept_info["crew"]:
                            st.write(f"- {crew}")
                    with col2:
                        st.write("**Equipment:**")
                        for equipment in dept_info["equipment"]:
                            st.write(f"- {equipment}")
                    st.write("**Notes:**")
                    for note in dept_info["notes"]:
                        st.write(f"- {note}")
    
    with tab7:
        st.write("## Call Sheets")
        if schedule_data and "schedule" in schedule_data:
            for day in schedule_data["schedule"]:
                st.write(f"### Day {day['day']} - {day['date']}")
                for scene in day["scenes"]:
                    with st.container(border=True):
                        st.write(f"**Scene {scene['scene_id']}**")
                        st.write(f"Location: {scene['location_id']}")
                        st.write(f"Time: {scene['start_time']} - {scene['end_time']}")
                        st.write(f"Setup: {scene['setup_time']}")
                        st.write(f"Wrap: {scene['wrap_time']}")
                        st.write("**Crew:**")
                        for crew_id in scene["crew_ids"]:
                            st.write(f"- {crew_id}")
                        st.write("**Equipment:**")
                        for equip_id in scene["equipment_ids"]:
                            st.write(f"- {equip_id}")
    
    with tab8:
        st.write("## Raw Data")
        if schedule_data:
            st.json(schedule_data)
            
            # Add download button
            st.download_button(
                label="Download Schedule Data",
                data=json.dumps(schedule_data, indent=2),
                file_name="schedule_data.json",
                mime="application/json"
            )
        else:
            st.warning("No schedule data found. Please generate a schedule first.")

def show_budget():
    st.header("Production Budget")
    
    # Load required data from previous steps
    script_results = load_from_storage('script_ingestion_results.json')
    character_results = load_from_storage('character_breakdown_results.json')
    schedule_results = load_from_storage('schedule_results.json')
    
    # Load budget results
    budget_results = load_from_storage('budget_results.json')
    
    if not script_results or not character_results or not schedule_results:
        st.warning("Please complete script analysis, character breakdown, and schedule generation first.")
        return
    
    # Add tabs for different views
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
        "Budget Overview", 
        "Location Details",
        "Equipment & Personnel",
        "Logistics & Insurance",
        "Vendor Analysis", 
        "Cash Flow", 
        "Scenario Analysis"
    ])
    
    # Check if we need to generate a new budget
    if not budget_results:
        with tab1:
            col1, col2 = st.columns([2, 1])
            with col1:
                target_budget = st.number_input("Target Budget (Optional)", min_value=0.0, step=1000.0)
            
            # Production constraints input
            with st.expander("Production Constraints", expanded=True):
                quality_level = st.selectbox("Production Quality Level", ["High", "Medium", "Low"], index=1)
                equipment_preference = st.selectbox("Equipment Preference", ["Premium", "Standard", "Budget"], index=1)
                crew_size = st.selectbox("Crew Size", ["Large", "Medium", "Small"], index=1)
                
                constraints = {
                    "quality_level": quality_level,
                    "equipment_preference": equipment_preference,
                    "crew_size": crew_size,
                    "schedule_days": len(schedule_results.get("schedule", [])),
                    "total_scenes": len(script_results.get("parsed_data", {}).get("scenes", [])),
                    "total_characters": len(character_results.get("characters", []))
                }
            
            if st.button("Generate Budget", type="primary"):
                with st.spinner("Generating production budget..."):
                    try:
                        # Prepare production data
                        production_data = {
                            "script_metadata": script_results.get("metadata", {}),
                            "scene_count": constraints["total_scenes"],
                            "character_count": constraints["total_characters"],
                            "schedule_days": constraints["schedule_days"],
                            "quality_level": quality_level
                        }
                        
                        # Prepare location data from schedule
                        location_data = {
                            "locations": [
                                scene.get("location", "Unknown")
                                for day in schedule_results.get("schedule", [])
                                if isinstance(day, dict)
                                for scene in day.get("scenes", [])
                                if isinstance(scene, dict)
                            ]
                        }
                        
                        # Prepare crew data
                        crew_data = {
                            "size": crew_size,
                            "equipment_level": equipment_preference,
                            "departments": ["Production", "Camera", "Lighting", "Sound", "Art", "Makeup", "Wardrobe"]
                        }
                        
                        # Generate budget using coordinator
                        budget_results = asyncio.run(budgeting_coordinator.initialize_budget(
                            production_data=production_data,
                            location_data=location_data,
                            crew_data=crew_data,
                            target_budget=target_budget if target_budget > 0 else None,
                            constraints=constraints
                        ))
                        
                        save_to_storage(budget_results, 'budget_results.json')
                        st.success("Budget generated!")
                        st.rerun()
                    except Exception as e:
                        logger.error(f"Error generating budget: {str(e)}", exc_info=True)
                        st.error(f"An error occurred: {str(e)}")
        return
    
    with tab1:
        # Display budget overview in a structured format
        if "total_estimates" in budget_results:
            st.header("Budget Overview")
            total = budget_results["total_estimates"]
            
            # Display grand total with large emphasis
            st.markdown(f"### 💰 Total Budget: ${total['grand_total']:,.2f}")
            
            # Create metrics for main categories
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Location Costs", f"${total['total_location_costs']:,.2f}")
            with col2:
                st.metric("Equipment Costs", f"${total['total_equipment_costs']:,.2f}")
            with col3:
                st.metric("Personnel Costs", f"${total['total_personnel_costs']:,.2f}")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Logistics Costs", f"${total['total_logistics_costs']:,.2f}")
            with col2:
                st.metric("Insurance Costs", f"${total['total_insurance_costs']:,.2f}")
            with col3:
                st.metric("Contingency", f"${total['contingency_amount']:,.2f}")
            
            # Add pie chart for cost distribution
            st.subheader("Cost Distribution")
            cost_data = {
                "Location": total['total_location_costs'],
                "Equipment": total['total_equipment_costs'],
                "Personnel": total['total_personnel_costs'],
                "Logistics": total['total_logistics_costs'],
                "Insurance": total['total_insurance_costs'],
                "Contingency": total['contingency_amount']
            }
            fig = px.pie(
                values=list(cost_data.values()),
                names=list(cost_data.keys()),
                title="Budget Distribution"
            )
            st.plotly_chart(fig)
    
    with tab2:
        st.header("📍 Location Costs Breakdown")
        if "location_costs" in budget_results:
            total_location = 0
            for loc_id, loc_data in budget_results["location_costs"].items():
                with st.expander(f"Location: {loc_id}", expanded=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Daily Rate", f"${loc_data['daily_rate']:,.2f}")
                        st.metric("Permit Costs", f"${loc_data['permit_costs']:,.2f}")
                    with col2:
                        st.metric("Total Days", loc_data['total_days'])
                        st.metric("Total Cost", f"${loc_data['total_cost']:,.2f}")
                    
                    if loc_data.get('additional_fees'):
                        st.write("**Additional Fees:**")
                        for fee in loc_data['additional_fees']:
                            st.write(f"- {fee}")
                    total_location += loc_data['total_cost']
            
            # Add location costs summary
            st.metric("Total Location Costs", f"${total_location:,.2f}")
    
    with tab3:
        st.header("Equipment & Personnel")
        
        # Equipment Costs
        st.subheader("🎥 Equipment Costs")
        if "equipment_costs" in budget_results:
            total_equipment = 0
            for equip_type, equip_data in budget_results["equipment_costs"].items():
                with st.expander(f"{equip_type.title()} Equipment", expanded=True):
                    if equip_data.get('items'):
                        st.write("**Items:**")
                        for item in equip_data['items']:
                            rate = equip_data['rental_rates'].get(item, 0)
                            st.write(f"- {item}: ${rate:,.2f}/day")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if equip_data.get('insurance_costs'):
                            st.metric("Insurance Costs", f"${equip_data['insurance_costs']:,.2f}")
                    with col2:
                        st.metric("Total Cost", f"${equip_data['total_cost']:,.2f}")
                    total_equipment += equip_data['total_cost']
            
            st.metric("Total Equipment Costs", f"${total_equipment:,.2f}")
        
        # Personnel Costs
        st.markdown("---")
        st.subheader("👥 Personnel Costs")
        if "personnel_costs" in budget_results:
            total_personnel = 0
            for role, role_data in budget_results["personnel_costs"].items():
                with st.expander(f"{role.title()}", expanded=True):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Daily Rate", f"${role_data['daily_rate']:,.2f}")
                        st.metric("Overtime Rate", f"${role_data['overtime_rate']:,.2f}")
                    with col2:
                        st.metric("Total Days", role_data['total_days'])
                        st.metric("Benefits", f"${role_data['benefits']:,.2f}")
                    st.metric("Total Cost", f"${role_data['total_cost']:,.2f}")
                    total_personnel += role_data['total_cost']
            
            st.metric("Total Personnel Costs", f"${total_personnel:,.2f}")
    
    with tab4:
        st.header("Logistics & Insurance")
        
        # Logistics Costs
        st.subheader("🚛 Logistics Costs")
        if "logistics_costs" in budget_results:
            logistics = budget_results["logistics_costs"]
            total_logistics = 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                if "transportation" in logistics:
                    st.write("**Transportation:**")
                    for item, cost in logistics["transportation"].items():
                        st.metric(item.title(), f"${cost:,.2f}")
                        total_logistics += cost
            
            with col2:
                if "accommodation" in logistics:
                    st.write("**Accommodation:**")
                    for item, cost in logistics["accommodation"].items():
                        st.metric(item.title(), f"${cost:,.2f}")
                        total_logistics += cost
            
            with col3:
                if "catering" in logistics:
                    st.write("**Catering:**")
                    for item, cost in logistics["catering"].items():
                        st.metric(item.title(), f"${cost:,.2f}")
                        total_logistics += cost
            
            if logistics.get("misc_expenses"):
                st.write("**Miscellaneous Expenses:**")
                for expense in logistics["misc_expenses"]:
                    st.write(f"- {expense}")
            
            st.metric("Total Logistics Costs", f"${total_logistics:,.2f}")
        
        # Insurance and Contingency
        st.markdown("---")
        st.subheader("🛡️ Insurance & Contingency")
        col1, col2 = st.columns(2)
        
        with col1:
            if "insurance_costs" in budget_results:
                st.write("**Insurance:**")
                total_insurance = 0
                for insurance_type, cost in budget_results["insurance_costs"].items():
                    st.metric(insurance_type.title(), f"${cost:,.2f}")
                    total_insurance += cost
                st.metric("Total Insurance Costs", f"${total_insurance:,.2f}")
        
        with col2:
            if "contingency" in budget_results:
                st.write("**Contingency:**")
                contingency = budget_results["contingency"]
                st.metric("Percentage", f"{contingency['percentage']}%")
                st.metric("Amount", f"${contingency['amount']:,.2f}")
    
    with tab5:
        st.header("Vendor Analysis")
        if budget_results and "vendor_status" in budget_results:
            vendor_status = budget_results["vendor_status"]
            
            # Summary metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Vendors", vendor_status.get("total_vendors", 0))
            with col2:
                st.metric("Total Spend", f"${vendor_status.get('total_spend', 0):,.2f}")
            with col3:
                st.metric("Outstanding Payments", f"${vendor_status.get('outstanding_payments', 0):,.2f}")
            
            # Vendor performance chart
            if "performance_summary" in vendor_status:
                st.subheader("Vendor Performance Scores")
                performance_data = pd.DataFrame(
                    vendor_status["performance_summary"].items(),
                    columns=["Vendor", "Score"]
                )
                st.bar_chart(performance_data.set_index("Vendor"))
            
            # Vendor details table
            if "spend_by_vendor" in vendor_status:
                st.subheader("Vendor Details")
                vendor_df = pd.DataFrame(vendor_status["spend_by_vendor"]).T
                st.dataframe(vendor_df)
        else:
            st.info("No vendor data available. Generate a budget first.")
    
    with tab6:
        st.header("Cash Flow Analysis")
        if budget_results and "cash_flow_status" in budget_results:
            cash_flow = budget_results["cash_flow_status"]
            
            # Cash flow health indicators
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Current Balance", f"${cash_flow.get('current_balance', 0):,.2f}")
            with col2:
                st.metric("Upcoming Payments", f"${cash_flow.get('upcoming_total', 0):,.2f}")
            
            # Health status
            health_status = cash_flow.get("health_status", "unknown")
            st.info(f"Cash Flow Health Status: {health_status.title()}")
            
            # Recommendations
            if "recommendations" in cash_flow:
                st.subheader("Recommendations")
                for rec in cash_flow["recommendations"]:
                    st.write(f"• {rec}")
            
            # Cash flow projection chart
            if "projections" in cash_flow:
                st.subheader("Cash Flow Projections")
                projection_data = pd.DataFrame(cash_flow["projections"])
                st.line_chart(projection_data.set_index("date")["balance"])
        else:
            st.info("No cash flow data available. Generate a budget first.")
    
    with tab7:
        st.header("Scenario Analysis")
        if budget_results:
            # Scenario selection
            scenario = st.selectbox(
                "Select Scenario",
                ["Base", "Optimistic", "Conservative", "Aggressive Cost Cutting"],
                index=0
            )
            
            # Scenario parameters
            with st.expander("Scenario Parameters", expanded=True):
                quality_impact = st.slider("Quality Impact Tolerance", 0, 100, 50)
                timeline_flexibility = st.slider("Timeline Flexibility (days)", 0, 30, 5)
                risk_tolerance = st.select_slider(
                    "Risk Tolerance",
                    options=["Low", "Medium", "High"],
                    value="Medium"
                )
            
            if st.button("Run Scenario Analysis"):
                with st.spinner("Analyzing scenario..."):
                    try:
                        # Prepare scenario constraints
                        scenario_constraints = {
                            "quality_impact_tolerance": quality_impact / 100,
                            "timeline_flexibility": timeline_flexibility,
                            "risk_tolerance": risk_tolerance.lower(),
                            "original_constraints": constraints
                        }
                        
                        # Run scenario analysis
                        scenario_results = asyncio.run(budgeting_coordinator.optimize_current_budget(
                            scenario_constraints,
                            scenario=scenario.lower()
                        ))
                        
                        if scenario_results:
                            # Display scenario comparison
                            st.subheader("Scenario Comparison")
                            
                            # Cost comparison
                            original_total = budget_results["total_estimates"]["grand_total"]
                            optimized_total = scenario_results["optimized_budget"]["total_estimates"]["grand_total"]
                            savings = original_total - optimized_total
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Original Total", f"${original_total:,.2f}")
                            with col2:
                                st.metric("Optimized Total", f"${optimized_total:,.2f}")
                            with col3:
                                st.metric("Potential Savings", f"${savings:,.2f}")
                            
                            # Impact analysis
                            if "impact_analysis" in scenario_results:
                                st.subheader("Impact Analysis")
                                impact = scenario_results["impact_analysis"]
                                
                                # Quality impact
                                st.write("**Quality Impact:**", impact.get("quality_impact", {}).get("level", "Unknown"))
                                
                                # Timeline impact
                                delay_days = impact.get("timeline_impact", {}).get("delay_days", 0)
                                st.write("**Timeline Impact:**", f"{delay_days} days")
                                
                                # Risk assessment
                                st.write("**Risk Assessment:**")
                                for risk in impact.get("risk_assessment", []):
                                    st.write(f"• {risk}")
                            
                            # Recommendations
                            if "recommendations" in scenario_results:
                                st.subheader("Recommendations")
                                for rec in scenario_results["recommendations"]:
                                    st.write(f"• {rec.get('action', '')}")
                                    st.write(f"  Priority: {rec.get('priority', 'Unknown')}")
                                    st.write(f"  Timeline: {rec.get('timeline', 'Unknown')}")
                    except Exception as e:
                        logger.error(f"Error in scenario analysis: {str(e)}", exc_info=True)
                        st.error(f"An error occurred: {str(e)}")
        else:
            st.info("No budget data available. Generate a budget first.")

def show_storyboard():
    st.header("Storyboard Generation")
    logger.info("Starting storyboard view")
    
    # Define the absolute path for storyboards
    STORYBOARD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "storage", "storyboards")
    os.makedirs(STORYBOARD_DIR, exist_ok=True)
    logger.info(f"Using storyboard directory: {STORYBOARD_DIR}")
    
    # Load required data from previous steps
    script_results = load_from_storage('script_ingestion_results.json')
    logger.info(f"Script results loaded: {bool(script_results)}")
    
    if not script_results:
        st.warning("Please complete script analysis first.")
        logger.warning("No script results found")
        return

    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["Storyboard View", "Shot Setup", "Export Options", "Settings"])
    
    with tab1:
        # Load storyboard results
        storyboard_results = load_from_storage('storyboard_results.json')
        logger.info(f"Storyboard results loaded: {bool(storyboard_results)}")
        
        # Add a prominent storyboard generation button at the top
        if not storyboard_results:
            st.subheader("Generate New Storyboard")
            if st.button("🎬 GENERATE STORYBOARD 🎬", key="main_storyboard_button", type="primary", use_container_width=True):
                logger.info("Starting storyboard generation")
                with st.spinner("Generating storyboard images..."):
                    try:
                        # Get current settings
                        settings = load_from_storage('storyboard_settings.json') or {}
                        logger.info(f"Loaded storyboard settings: {bool(settings)}")
                        
                        # Generate storyboard using coordinator with settings
                        storyboard_results = asyncio.run(
                            storyboard_coordinator.generate_storyboard(
                                script_results,
                                shot_settings=settings.get("shot_settings", {})
                            )
                        )
                        
                        # Update image paths to absolute paths
                        if "scenes" in storyboard_results:
                            for scene in storyboard_results["scenes"]:
                                if "image_path" in scene:
                                    scene["image_path"] = os.path.join(STORYBOARD_DIR, f"scene_{scene.get('scene_id', '')}.webp")
                                    logger.info(f"Updated image path for scene {scene.get('scene_id', '')}: {scene['image_path']}")
                        
                        save_to_storage(storyboard_results, 'storyboard_results.json')
                        logger.info("Storyboard generation completed successfully")
                        st.success("Storyboard generated!")
                        st.rerun()
                    except Exception as e:
                        logger.error(f"Error generating storyboard: {str(e)}", exc_info=True)
                        st.error(f"An error occurred: {str(e)}")
        
        if storyboard_results:
            # Display storyboard images
            if "scenes" in storyboard_results:
                st.subheader("Storyboard Sequence")
                logger.info(f"Number of scenes in storyboard: {len(storyboard_results['scenes'])}")
                
                # Add sequence controls
                col1, col2 = st.columns([3, 1])
                with col1:
                    view_mode = st.radio("View Mode", ["Grid", "Slideshow"], horizontal=True)
                    logger.info(f"Selected view mode: {view_mode}")
                
                if view_mode == "Grid":
                    # Organize scenes into rows
                    scenes = storyboard_results.get("scenes", [])
                    num_cols = st.select_slider("Panels per row", options=[2, 3, 4], value=3)
                    logger.info(f"Grid view: {num_cols} panels per row")
                    
                    for i in range(0, len(scenes), num_cols):
                        row_scenes = scenes[i:i+num_cols]
                        cols = st.columns(num_cols)
                        
                        for j, scene in enumerate(row_scenes):
                            with cols[j]:
                                # Get the absolute path for the image
                                image_path = os.path.join(STORYBOARD_DIR, f"scene_{scene.get('scene_id', '')}.webp")
                                logger.info(f"Checking image path for scene {scene.get('scene_id', '')}: {image_path}")
                                
                                if os.path.exists(image_path):
                                    logger.info(f"Image found at path: {image_path}")
                                    st.image(image_path, 
                                           caption=f"Scene {scene.get('scene_id', '?')} - {scene.get('technical_params', {}).get('shot_type', 'MS')}")
                                    
                                    # Update the path in the scene data
                                    scene["image_path"] = image_path
                                    logger.info(f"Updated image path in scene data to: {image_path}")
                                    
                                    # Display prompt
                                    with st.expander("View Prompt"):
                                        st.write("**Original Prompt:**")
                                        st.write(scene.get("prompt", "No prompt available"))
                                        if "enhanced_prompt" in scene:
                                            st.write("**Enhanced Prompt:**")
                                            st.write(scene["enhanced_prompt"])
                                else:
                                    logger.error(f"Image not found at path: {image_path}")
                                    st.error(f"Scene {scene.get('scene_id', '?')} - Image not found")
                else:  # Slideshow mode
                    scenes = storyboard_results.get("scenes", [])
                    if not scenes:
                        st.warning("No scenes found in storyboard.")
                        return
                    
                    # Initialize current scene index in session state if not present
                    if 'current_scene_index' not in st.session_state:
                        st.session_state.current_scene_index = 0
                    
                    current_scene = st.session_state.current_scene_index
                    
                    # Navigation controls
                    col1, col2, col3 = st.columns([1, 4, 1])
                    
                    with col1:
                        if st.button("⬅️ Previous") and current_scene > 0:
                            st.session_state.current_scene_index = current_scene - 1
                            st.rerun()
                    
                    with col2:
                        scene = scenes[current_scene]
                        st.write(f"### Scene {scene.get('scene_id', '?')}")
                        
                        # Get the absolute path for the image
                        image_path = os.path.join(STORYBOARD_DIR, f"scene_{scene.get('scene_id', '')}.webp")
                        
                        if os.path.exists(image_path):
                            st.image(image_path, use_column_width=True)
                            
                            # Display scene details
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write("**Scene Description:**")
                                st.write(scene.get("description", "No description available"))
                            with col2:
                                st.write("**Technical Notes:**")
                                st.write(scene.get("technical_notes", "No technical notes available"))
                            
                            # Display prompt
                            with st.expander("View Prompt"):
                                st.write("**Original Prompt:**")
                                st.write(scene.get("prompt", "No prompt available"))
                                if "enhanced_prompt" in scene:
                                    st.write("**Enhanced Prompt:**")
                                    st.write(scene["enhanced_prompt"])
                        else:
                            st.error(f"Scene {scene.get('scene_id', '?')} - Image not found")
                    
                    with col3:
                        if st.button("Next ➡️") and current_scene < len(scenes) - 1:
                            st.session_state.current_scene_index = current_scene + 1
                            st.rerun()
                    
                    # Progress bar
                    st.progress((current_scene + 1) / len(scenes))
                
                # Add regenerate button
                if st.button("Regenerate Storyboard", key="regenerate_button", type="primary"):
                    with st.spinner("Regenerating storyboard images..."):
                        try:
                            settings = load_from_storage('storyboard_settings.json') or {}
                            storyboard_results = asyncio.run(
                                storyboard_coordinator.generate_storyboard(
                                    script_results,
                                    shot_settings=settings.get("shot_settings", {})
                                )
                            )
                            save_to_storage(storyboard_results, 'storyboard_results.json')
                            st.success("Storyboard regenerated!")
                            st.rerun()
                        except Exception as e:
                            logger.error(f"Error regenerating storyboard: {str(e)}", exc_info=True)
                            st.error(f"An error occurred: {str(e)}")
    
    with tab2:
        st.subheader("Shot Setup")
        
        # Load or initialize settings
        settings = load_from_storage('storyboard_settings.json') or {}
        shot_settings = settings.get("shot_settings", {})
        
        # Global shot settings
        st.write("### Global Shot Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            default_shot = st.selectbox(
                "Default Shot Type",
                ["MS", "WS", "CU", "ECU", "OTS", "POV"],
                index=0
            )
            style = st.selectbox(
                "Visual Style",
                ["realistic", "scribble", "noir", "anime", "watercolor", "storyboard"],
                index=0
            )
        
        with col2:
            mood = st.selectbox(
                "Default Mood",
                ["neutral", "dramatic", "tense", "joyful", "mysterious", "melancholic"],
                index=0
            )
            camera_angle = st.selectbox(
                "Default Camera Angle",
                ["eye_level", "low_angle", "high_angle", "dutch_angle"],
                index=0
            )
        
        # Scene-specific settings
        st.write("### Scene-Specific Settings")
        if script_results and "scenes" in script_results:
            scene_settings = {}
            for scene in script_results["scenes"]:
                scene_id = scene.get("scene_id", "")
                if scene_id:
                    with st.expander(f"Scene {scene_id}"):
                        col1, col2 = st.columns(2)
                        with col1:
                            scene_shot = st.selectbox(
                                "Shot Type",
                                ["MS", "WS", "CU", "ECU", "OTS", "POV"],
                                key=f"shot_{scene_id}"
                            )
                            scene_style = st.selectbox(
                                "Style",
                                ["realistic", "scribble", "noir", "anime", "watercolor", "storyboard"],
                                key=f"style_{scene_id}"
                            )
                        with col2:
                            scene_mood = st.selectbox(
                                "Mood",
                                ["neutral", "dramatic", "tense", "joyful", "mysterious", "melancholic"],
                                key=f"mood_{scene_id}"
                            )
                            scene_camera = st.selectbox(
                                "Camera Angle",
                                ["eye_level", "low_angle", "high_angle", "dutch_angle"],
                                key=f"camera_{scene_id}"
                            )
                        
                        scene_settings[scene_id] = {
                            "shot_type": scene_shot,
                            "style": scene_style,
                            "mood": scene_mood,
                            "camera_angle": scene_camera
                        }
        
        # Save settings button
        if st.button("Save Shot Settings", type="primary"):
            settings["shot_settings"] = {
                "default_shot_type": default_shot,
                "style": style,
                "mood": mood,
                "camera_angle": camera_angle,
                "scene_settings": scene_settings
            }
            save_to_storage(settings, 'storyboard_settings.json')
            st.success("Shot settings saved! They will be applied to the next storyboard generation.")
    
    with tab3:
        st.subheader("Export Options")
        
        if not storyboard_results:
            st.warning("Generate a storyboard first to enable export options.")
        else:
            export_format = st.radio("Export Format", ["PDF", "Slideshow"], horizontal=True)
            
            col1, col2 = st.columns(2)
            with col1:
                include_annotations = st.checkbox("Include Annotations", value=True)
                include_technical = st.checkbox("Include Technical Notes", value=True)
            with col2:
                include_descriptions = st.checkbox("Include Scene Descriptions", value=True)
                high_quality = st.checkbox("High Quality Export", value=True)
            
            if st.button("Export Storyboard", type="primary"):
                try:
                    with st.spinner("Exporting storyboard..."):
                        # Create export settings
                        export_settings = {
                            "include_annotations": include_annotations,
                            "include_technical": include_technical,
                            "include_descriptions": include_descriptions,
                            "quality": "hd" if high_quality else "standard"
                        }
                        
                        # Generate timestamp for filename
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        
                        if export_format == "PDF":
                            output_path = f"data/exports/storyboard_{timestamp}.pdf"
                        else:
                            output_path = f"data/exports/storyboard_{timestamp}"
                        
                        # Export using coordinator
                        exported_path = asyncio.run(
                            storyboard_coordinator.export_storyboard(
                                storyboard_results,
                                export_format.lower(),
                                output_path
                            )
                        )
                        
                        if export_format == "PDF":
                            with open(exported_path, "rb") as f:
                                st.download_button(
                                    label="Download PDF",
                                    data=f.read(),
                                    file_name=f"storyboard_{timestamp}.pdf",
                                    mime="application/pdf"
                                )
                        else:
                            st.success(f"Slideshow exported to: {exported_path}")
                            st.write("You can access the slideshow at:")
                            st.code(f"/exports/storyboard_{timestamp}/slideshow.html")
                
                except Exception as e:
                    st.error(f"Error exporting storyboard: {str(e)}")
    
    with tab4:
        st.subheader("Storyboard Settings")
        
        # Load or initialize settings
        settings = load_from_storage('storyboard_settings.json') or {}
        
        # Layout settings
        st.write("### Layout Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            panels_per_row = st.select_slider(
                "Panels per Row",
                options=[2, 3, 4],
                value=settings.get("layout", {}).get("panels_per_row", 3)
            )
            panel_size = st.select_slider(
                "Panel Size",
                options=["small", "medium", "large"],
                value=settings.get("layout", {}).get("panel_size", "medium")
            )
        
        with col2:
            show_captions = st.checkbox(
                "Show Captions",
                value=settings.get("layout", {}).get("show_captions", True)
            )
            show_technical = st.checkbox(
                "Show Technical Info",
                value=settings.get("layout", {}).get("show_technical", True)
            )
        
        # Image settings
        st.write("### Image Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            image_quality = st.select_slider(
                "Image Quality",
                options=["standard", "hd"],
                value=settings.get("image", {}).get("quality", "standard")
            )
            aspect_ratio = st.selectbox(
                "Aspect Ratio",
                ["1:1", "16:9", "4:3", "2.35:1"],
                index=["1:1", "16:9", "4:3", "2.35:1"].index(
                    settings.get("image", {}).get("aspect_ratio", "16:9")
                )
            )
        
        with col2:
            color_mode = st.selectbox(
                "Color Mode",
                ["color", "grayscale", "sepia"],
                index=["color", "grayscale", "sepia"].index(
                    settings.get("image", {}).get("color_mode", "color")
                )
            )
            border_style = st.selectbox(
                "Border Style",
                ["none", "thin", "thick", "double"],
                index=["none", "thin", "thick", "double"].index(
                    settings.get("image", {}).get("border", "thin")
                )
            )
        
        # Save settings button
        if st.button("Save Settings", type="primary"):
            settings.update({
                "layout": {
                    "panels_per_row": panels_per_row,
                    "panel_size": panel_size,
                    "show_captions": show_captions,
                    "show_technical": show_technical
                },
                "image": {
                    "quality": image_quality,
                    "aspect_ratio": aspect_ratio,
                    "color_mode": color_mode,
                    "border": border_style
                }
            })
            save_to_storage(settings, 'storyboard_settings.json')
            st.success("Settings saved! They will be applied to the next storyboard generation.")

def show_overview():
    st.header("Project Overview")
    results = {
        'script': load_from_storage('script_ingestion_results.json'),
        'one_liner': load_from_storage('one_liner_results.json'),
        'characters': load_from_storage('character_breakdown_results.json'),
        'schedule': load_from_storage('schedule_results.json'),
        'budget': load_from_storage('budget_results.json'),
        'storyboard': load_from_storage('storyboard_results.json')
    }
    
    if all(results.values()):
        st.subheader("Script Analysis")
        st.json(results['script'])
        
        st.subheader("One-Liner")
        st.json(results['one_liner'])
        
        st.subheader("Character Breakdown")
        st.json(results['characters'])
        
        st.subheader("Production Schedule")
        st.json(results['schedule'])
        
        st.subheader("Budget")
        st.json(results['budget'])
        
        st.subheader("Storyboard")
        st.json(results['storyboard'])
    else:
        st.warning("Please complete all previous steps first.")

def get_location_color(location):
    """Generate a consistent color for a given location."""
    if not location:
        return "#808080"  # Default gray
    
    # Use hash of location name to generate consistent colors
    hash_val = hash(location)
    # List of visually distinct colors
    colors = [
        "#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEEAD",
        "#D4A5A5", "#9B59B6", "#3498DB", "#E67E22", "#2ECC71"
    ]
    return colors[abs(hash_val) % len(colors)]

def display_week_calendar(events, week_start):
    """Display a week view of the calendar with drag-and-drop support."""
    # Convert week_start to datetime if it's a string
    if isinstance(week_start, str):
        week_start = datetime.strptime(week_start, "%Y-%m-%d")
    
    # Create week days
    week_days = [(week_start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
    
    # Create time slots (30-minute intervals)
    time_slots = []
    start_time = datetime.strptime("06:00", "%H:%M")
    for _ in range(32):  # 6 AM to 10 PM
        time_slots.append(start_time.strftime("%H:%M"))
        start_time += timedelta(minutes=30)
    
    # Create grid
    st.write("### Week Calendar")
    
    # Header row with dates
    header_cols = st.columns(8)
    with header_cols[0]:
        st.write("Time")
    for i, day in enumerate(week_days):
        with header_cols[i + 1]:
            st.write(datetime.strptime(day, "%Y-%m-%d").strftime("%a %m/%d"))
    
    # Time slots and events
    for time_slot in time_slots:
        slot_cols = st.columns(8)
        with slot_cols[0]:
            st.write(time_slot)
        
        for i, day in enumerate(week_days):
            with slot_cols[i + 1]:
                # Find events that overlap with this time slot
                slot_events = [
                    event for event in events
                    if event["start"].split("T")[0] == day
                    and time_slot >= event["start"].split("T")[1][:5]
                    and time_slot <= event["end"].split("T")[1][:5]
                ]
                
                if slot_events:
                    for event in slot_events:
                        with st.container(border=True):
                            st.markdown(
                                f'<div style="background-color: {event["color"]}; padding: 5px; border-radius: 3px;">'
                                f'Scene {event["extendedProps"]["scene_id"]}<br>'
                                f'{event["extendedProps"]["location"]}'
                                '</div>',
                                unsafe_allow_html=True
                            )
                            
                            # Make container draggable
                            if st.button(f"🔄 Move {event['title']}", key=f"move_{event['id']}_{time_slot}_{day}"):
                                st.session_state.dragged_scene = event
                                st.session_state.schedule_modified = True

def display_day_calendar(events, day):
    """Display a detailed day view of the calendar."""
    st.write(f"### {datetime.strptime(day, '%Y-%m-%d').strftime('%A, %B %d, %Y')}")
    
    # Create time slots (15-minute intervals)
    time_slots = []
    start_time = datetime.strptime("06:00", "%H:%M")
    for _ in range(64):  # 6 AM to 10 PM
        time_slots.append(start_time.strftime("%H:%M"))
        start_time += timedelta(minutes=15)
    
    # Group events by location
    location_events = {}
    for event in events:
        location = event["extendedProps"]["location"]
        if location not in location_events:
            location_events[location] = []
        location_events[location].append(event)
    
    # Create columns for each location
    if location_events:
        location_cols = st.columns(len(location_events))
        for i, (location, loc_events) in enumerate(location_events.items()):
            with location_cols[i]:
                st.write(f"**{location}**")
                
                # Display events for this location
                for event in sorted(loc_events, key=lambda x: x["start"]):
                    with st.container(border=True):
                        st.markdown(
                            f'<div style="background-color: {event["color"]}; padding: 10px; border-radius: 5px;">'
                            f'<b>{event["title"]}</b><br>'
                            f'{event["start"].split("T")[1]} - {event["end"].split("T")[1]}<br>'
                            f'Cast: {", ".join(event["extendedProps"]["cast"][:3])}'
                            f'{"..." if len(event["extendedProps"]["cast"]) > 3 else ""}'
                            '</div>',
                            unsafe_allow_html=True
                        )
                        
                        # Add move button
                        if st.button(f"🔄 Move {event['title']}", key=f"move_{event['id']}_{location}"):
                            st.session_state.dragged_scene = event
                            st.session_state.schedule_modified = True
    else:
        st.info("No events scheduled for this day")

def download_image_from_replicate(output, save_path):
    """Helper function to download and save images from Replicate output."""
    try:
        # Handle both single URL and FileOutput cases
        if isinstance(output, str):
            # If output is a direct URL string
            response = httpx.get(output)
            if response.status_code == 200:
                with open(save_path, "wb") as f:
                    f.write(response.content)
                return True
        elif hasattr(output, 'read'):
            # If output is a FileOutput object
            with open(save_path, "wb") as f:
                f.write(output.read())
            return True
        return False
    except Exception as e:
        logger.error(f"Error downloading image: {str(e)}")
        return False

async def generate_image(prompt, scene_id):
    """Generate image using Replicate API."""
    try:
        # Configure the model input
        input_data = {
            "prompt": prompt,
            "num_inference_steps": 50,
            "guidance_scale": 7.5
        }
        
        # Run the model
        output = replicate.run(
            "black-forest-labs/flux-schnell",
            input=input_data
        )
        
        # Handle the output
        if output:
            # Create the save directory if it doesn't exist
            save_dir = os.path.join("static", "storage", "storyboards")
            os.makedirs(save_dir, exist_ok=True)
            
            # Save each output image
            saved_paths = []
            for i, item in enumerate(output):
                save_path = os.path.join(save_dir, f"scene_{scene_id}_{i}.webp")
                if download_image_from_replicate(item, save_path):
                    saved_paths.append(save_path)
            
            return saved_paths[0] if saved_paths else None
            
    except Exception as e:
        logger.error(f"Error generating image for scene {scene_id}: {str(e)}")
        return None

if __name__ == '__main__':
    logger.info("Application started")
    main() 